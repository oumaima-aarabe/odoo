# Part of Odoo. See LICENSE file for full copyright and licensing details.
import logging
import random
import threading
import time
from collections.abc import Mapping, Sequence
from functools import partial

from psycopg2 import IntegrityError, OperationalError, errorcodes, errors

import odoo
from odoo.exceptions import UserError, ValidationError
from odoo.http import request
from odoo.models import check_method_name
from odoo.modules.registry import Registry
from odoo.tools import DotDict, lazy
from odoo.tools.translate import translate_sql_constraint

from . import security

_logger = logging.getLogger(__name__)

PG_CONCURRENCY_ERRORS_TO_RETRY = (errorcodes.LOCK_NOT_AVAILABLE, errorcodes.SERIALIZATION_FAILURE, errorcodes.DEADLOCK_DETECTED)
PG_CONCURRENCY_EXCEPTIONS_TO_RETRY = (errors.LockNotAvailable, errors.SerializationFailure, errors.DeadlockDetected)
MAX_TRIES_ON_CONCURRENCY_FAILURE = 5


def dispatch(method, params):
    db, uid, passwd = params[0], int(params[1]), params[2]
    security.check(db, uid, passwd)

    threading.current_thread().dbname = db
    threading.current_thread().uid = uid
    registry = Registry(db).check_signaling()
    with registry.manage_changes():
        if method == 'execute':
            res = execute(db, uid, *params[3:])
        elif method == 'execute_kw':
            res = execute_kw(db, uid, *params[3:])
        else:
            raise NameError("Method not available %s" % method)
    return res


def execute_cr(cr, uid, obj, method, *args, **kw):
    # clean cache etc if we retry the same transaction
    cr.reset()
    env = odoo.api.Environment(cr, uid, {})
    recs = env.get(obj)
    if recs is None:
        raise UserError(env._("Object %s doesn't exist", obj))
    result = retrying(partial(odoo.api.call_kw, recs, method, args, kw), env)
    # force evaluation of lazy values before the cursor is closed, as it would
    # error afterwards if the lazy isn't already evaluated (and cached)
    for l in _traverse_containers(result, lazy):
        _0 = l._value
    return result


def execute_kw(db, uid, obj, method, args, kw=None):
    return execute(db, uid, obj, method, *args, **kw or {})


def execute(db, uid, obj, method, *args, **kw):
    # TODO could be conditionnaly readonly as in _call_kw_readonly
    with Registry(db).cursor() as cr:
        check_method_name(method)
        res = execute_cr(cr, uid, obj, method, *args, **kw)
        if res is None:
            _logger.info('The method %s of the object %s can not return `None`!', method, obj)
        return res


def _as_validation_error(env, exc):
    """ Return the IntegrityError encapsulated in a nicely formatted ValidationError. """

    unknown = env._('Unknown')
    model = DotDict({'_name': 'unknown', '_description': unknown})
    field = DotDict({'name': 'unknown', 'string': unknown})

    # Find the model and field based on the exception details
    for _name, rclass in env.registry.items():
        if exc.diag.table_name == rclass._table:
            model = rclass
            field = model._fields.get(exc.diag.column_name, field)
            break  # Exit loop once model and field are found

    # Match specific database errors
    match exc:
        case errors.NotNullViolation():
            return ValidationError(env._(
                "The operation cannot be completed:\n"
                "- Create/update: a mandatory field is not set.\n"
                "- Delete: another model requires the record being deleted. "
                "If possible, archive it instead.\n\n"
                "Model: %(model_name)s (%(model_tech_name)s)\n"
                "Field: %(field_name)s (%(field_tech_name)s)\n",
                model_name=model._description,
                model_tech_name=model._name,
                field_name=field.string,
                field_tech_name=field.name,
            ))

        case errors.ForeignKeyViolation():
            return ValidationError(env._(
                "The operation cannot be completed: another model requires "
                "the record being deleted. If possible, archive it instead.\n\n"
                "Model: %(model_name)s (%(model_tech_name)s)\n"
                "Constraint: %(constraint)s\n",
                model_name=model._description,
                model_tech_name=model._name,
                constraint=exc.diag.constraint_name,
            ))

    # Handle SQL constraint violation
    constraint_name = exc.diag.constraint_name
    if constraint_name in env.registry._sql_constraints:
        translated_constraint = translate_sql_constraint(env.cr, constraint_name, env.context.get('lang', 'en_US'))
        return ValidationError(env._(
            "The operation cannot be completed: %s", translated_constraint
        ))

    # Generic error fallback
    return ValidationError(env._("The operation cannot be completed: %s", exc.args[0]))



def retrying(func, env):
    """
    Retry calling the function on serialization failures. It retries up
    to a maximum of 5 times with exponential backoff.
    
    :param callable func: The function to call.
    :param odoo.api.Environment env: The environment where the registry
        and the cursor are taken.
    """
    MAX_RETRIES = MAX_TRIES_ON_CONCURRENCY_FAILURE

    for attempt in range(1, MAX_RETRIES + 1):
        remaining_tries = MAX_RETRIES - attempt
        try:
            result = func()
            
            if not env.cr._closed:
                env.cr.flush()  # flush changes to the database
                env.cr.commit()  # commit the transaction
            env.registry.signal_changes()  # signal any registry changes
            return result  # exit after success

        except (IntegrityError, OperationalError) as exc:
            if env.cr._closed:
                raise

            env.cr.rollback()  # rollback transaction on failure
            env.reset()
            env.registry.reset_changes()

            # Handle request rewinding for file uploads (if applicable)
            if request:
                request.session = request._get_session_and_dbname()[0]
                for filename, file in request.httprequest.files.items():
                    if hasattr(file, "seekable") and file.seekable():
                        file.seek(0)
                    else:
                        raise RuntimeError(f"Cannot retry request on input file {filename!r}") from exc

            # IntegrityError should not be retried
            if isinstance(exc, IntegrityError):
                raise _as_validation_error(env, exc) from exc

            # Check if exception is retryable
            if not isinstance(exc, PG_CONCURRENCY_EXCEPTIONS_TO_RETRY):
                raise

            if remaining_tries == 0:
                _logger.info("%s, maximum retry limit reached!", errorcodes.lookup(exc.pgcode))
                raise

            # Exponential backoff with jitter
            wait_time = random.uniform(0.5, 2 ** attempt)
            _logger.info("%s, %s retries left, retrying in %.04f seconds...", errorcodes.lookup(exc.pgcode), remaining_tries, wait_time)
            time.sleep(wait_time)

    # If all retries are exhausted, raise an exception
    raise RuntimeError("Exceeded maximum retry attempts")




def _traverse_containers(val, type_):
    """ Yields atoms filtered by specified ``type_`` (or type tuple), traverses
    through standard containers (non-string mappings or sequences) *unless*
    they're selected by the type filter
    """
    from odoo.models import BaseModel
    if isinstance(val, type_):
        yield val
    elif isinstance(val, (str, bytes, BaseModel)):
        return
    elif isinstance(val, Mapping):
        for k, v in val.items():
            yield from _traverse_containers(k, type_)
            yield from _traverse_containers(v, type_)
    elif isinstance(val, Sequence):
        for v in val:
            yield from _traverse_containers(v, type_)
