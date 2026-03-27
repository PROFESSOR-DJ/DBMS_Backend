/**
 * utils/errorHandler.js
 * 
 * Centralised error-handling utilities.
 * 
 * - AppError        : typed application error with HTTP status
 * - handleMySQLError: maps MySQL error codes to AppError
 * - handleMongoError: maps MongoDB error codes to AppError
 * - asyncHandler    : wraps async route handlers — no more
 *                     repetitive try/catch in every controller
 * - sendError       : uniform JSON error response
 */

// ─────────────────────────────────────────────────────────────
// MySQL error codes we care about
// ─────────────────────────────────────────────────────────────
const MYSQL_ERRORS = {
  ER_DUP_ENTRY:             1062,   // UNIQUE constraint violation
  ER_NO_REFERENCED_ROW:     1216,   // FK insert: parent row missing
  ER_NO_REFERENCED_ROW_2:   1452,   // FK insert: parent row missing (InnoDB)
  ER_ROW_IS_REFERENCED:     1217,   // FK delete: child rows exist
  ER_ROW_IS_REFERENCED_2:   1451,   // FK delete: child rows exist (InnoDB)
  ER_BAD_NULL_ERROR:        1048,   // NOT NULL violation
  ER_DATA_TOO_LONG:         1406,   // Column value too long
  ER_SIGNAL_EXCEPTION:      1644,   // SIGNAL SQLSTATE from trigger / procedure
  ER_LOCK_DEADLOCK:         1213,   // Deadlock — retry-able
  ER_LOCK_WAIT_TIMEOUT:     1205,   // Lock wait timeout — retry-able
  ER_NO_SUCH_TABLE:         1146,   // Table does not exist
  ER_ACCESS_DENIED_ERROR:   1045,   // Auth failure
};

// ─────────────────────────────────────────────────────────────
// MongoDB error codes
// ─────────────────────────────────────────────────────────────
const MONGO_ERRORS = {
  DUPLICATE_KEY:       11000,   // Unique index violation
  DUPLICATE_KEY_E:     11001,   // Legacy duplicate key
  BULK_WRITE_ERROR:    'BulkWriteError',
  VALIDATION_ERROR:    121,     // Document validation failed
  CURSOR_NOT_FOUND:    43,      // Cursor expired
  NAMESPACE_NOT_FOUND: 26,      // Collection does not exist
};

// ─────────────────────────────────────────────────────────────
// AppError — enriched Error with HTTP status and error code
// ─────────────────────────────────────────────────────────────
class AppError extends Error {
  /**
   * @param {string} message   - Human-readable message
   * @param {number} status    - HTTP status code (400, 404, 409 …)
   * @param {string} code      - Machine-readable code for clients
   * @param {boolean} isRetryable - Hint to caller that a retry may succeed
   */
  constructor(message, status = 500, code = 'INTERNAL_ERROR', isRetryable = false) {
    super(message);
    this.name       = 'AppError';
    this.status     = status;
    this.code       = code;
    this.isRetryable = isRetryable;
    // Capture stack excluding this constructor frame
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, AppError);
    }
  }
}

// ─────────────────────────────────────────────────────────────
// handleMySQLError
// Converts a raw mysql2 error into an AppError with the right
// HTTP status and a message safe to send to the client.
// ─────────────────────────────────────────────────────────────
const handleMySQLError = (err) => {
  const errno = err.errno || err.code;

  switch (errno) {
    // ── Duplicate entry (e.g. duplicate paper_id or email) ──
    case MYSQL_ERRORS.ER_DUP_ENTRY: {
      // mysql2 message format: "Duplicate entry '<value>' for key '<key>'"
      const match = err.message.match(/Duplicate entry '(.+?)' for key '(.+?)'/);
      const value = match ? match[1] : 'unknown';
      const key   = match ? match[2] : 'unknown';
      return new AppError(
        `Duplicate value '${value}' for field '${key}'. Record already exists.`,
        409,
        'DUPLICATE_ENTRY'
      );
    }

    // ── FK violation on INSERT / UPDATE ──
    case MYSQL_ERRORS.ER_NO_REFERENCED_ROW:
    case MYSQL_ERRORS.ER_NO_REFERENCED_ROW_2:
      return new AppError(
        'Referenced record does not exist. Check that the related entity (journal, author, paper) exists first.',
        400,
        'FK_REFERENCE_NOT_FOUND'
      );

    // ── FK violation on DELETE ──
    case MYSQL_ERRORS.ER_ROW_IS_REFERENCED:
    case MYSQL_ERRORS.ER_ROW_IS_REFERENCED_2:
      return new AppError(
        'Cannot delete: this record is still referenced by other records. Remove child records first.',
        409,
        'FK_REFERENCE_EXISTS'
      );

    // ── NOT NULL violation ──
    case MYSQL_ERRORS.ER_BAD_NULL_ERROR: {
      const colMatch = err.message.match(/Column '(.+?)' cannot be null/);
      const col      = colMatch ? colMatch[1] : 'unknown';
      return new AppError(
        `Field '${col}' is required and cannot be null.`,
        400,
        'NULL_CONSTRAINT'
      );
    }

    // ── Value too long ──
    case MYSQL_ERRORS.ER_DATA_TOO_LONG: {
      const colMatch = err.message.match(/Data too long for column '(.+?)'/);
      const col      = colMatch ? colMatch[1] : 'unknown';
      return new AppError(
        `Value for field '${col}' is too long.`,
        400,
        'DATA_TOO_LONG'
      );
    }

    // ── SIGNAL from trigger or stored procedure ──
    case MYSQL_ERRORS.ER_SIGNAL_EXCEPTION:
      return new AppError(
        err.message,   // message comes directly from the SIGNAL statement
        409,
        'BUSINESS_RULE_VIOLATION'
      );

    // ── Deadlock — transient, safe to retry ──
    case MYSQL_ERRORS.ER_LOCK_DEADLOCK:
      return new AppError(
        'Database deadlock detected. Please retry the operation.',
        503,
        'DEADLOCK',
        true  // isRetryable
      );

    // ── Lock wait timeout — transient ──
    case MYSQL_ERRORS.ER_LOCK_WAIT_TIMEOUT:
      return new AppError(
        'Database lock wait timeout. Please retry the operation.',
        503,
        'LOCK_TIMEOUT',
        true
      );

    // ── Table missing ──
    case MYSQL_ERRORS.ER_NO_SUCH_TABLE:
      return new AppError(
        'Database table not found. Run the schema migration script.',
        500,
        'TABLE_NOT_FOUND'
      );

    // ── Auth failure ──
    case MYSQL_ERRORS.ER_ACCESS_DENIED_ERROR:
      return new AppError(
        'Database authentication failed.',
        500,
        'DB_AUTH_FAILED'
      );

    // ── Unknown MySQL error ──
    default:
      return new AppError(
        `Database error: ${err.message}`,
        500,
        'DB_ERROR'
      );
  }
};

// ─────────────────────────────────────────────────────────────
// handleMongoError
// Converts a raw MongoDB error into an AppError.
// ─────────────────────────────────────────────────────────────
const handleMongoError = (err) => {
  const code     = err.code;
  const errName  = err.constructor?.name || '';

  // ── Duplicate key (unique index) ──
  if (code === MONGO_ERRORS.DUPLICATE_KEY || code === MONGO_ERRORS.DUPLICATE_KEY_E) {
    // keyValue shape: { paper_id: 'abc123' }
    const field = err.keyValue ? Object.keys(err.keyValue)[0] : 'unknown';
    const value = err.keyValue ? Object.values(err.keyValue)[0] : 'unknown';
    return new AppError(
      `Duplicate value '${value}' for field '${field}'. Record already exists.`,
      409,
      'DUPLICATE_ENTRY'
    );
  }

  // ── BulkWriteError (insertMany / bulkWrite) ──
  if (errName === MONGO_ERRORS.BULK_WRITE_ERROR || err instanceof Error && err.name === 'BulkWriteError') {
    const writeErrors = err.writeErrors || [];
    const dupCount    = writeErrors.filter(e => e.code === MONGO_ERRORS.DUPLICATE_KEY).length;
    const otherCount  = writeErrors.length - dupCount;

    if (dupCount > 0 && otherCount === 0) {
      return new AppError(
        `Bulk insert: ${dupCount} duplicate record(s) skipped.`,
        409,
        'BULK_DUPLICATE'
      );
    }
    return new AppError(
      `Bulk write error: ${dupCount} duplicate(s), ${otherCount} other error(s).`,
      400,
      'BULK_WRITE_ERROR'
    );
  }

  // ── Document validation failed ──
  if (code === MONGO_ERRORS.VALIDATION_ERROR) {
    return new AppError(
      `Document validation failed: ${err.message}`,
      400,
      'VALIDATION_ERROR'
    );
  }

  // ── MongoDB not connected ──
  if (errName === 'MongoNotConnectedError' || err.message?.includes('not connected')) {
    return new AppError(
      'MongoDB is not connected. Check the database connection.',
      503,
      'MONGO_NOT_CONNECTED'
    );
  }

  // ── Network / timeout ──
  if (errName === 'MongoNetworkError' || errName === 'MongoTimeoutError') {
    return new AppError(
      'MongoDB network error. Please retry.',
      503,
      'MONGO_NETWORK_ERROR',
      true
    );
  }

  // ── Namespace (collection) not found ──
  if (code === MONGO_ERRORS.NAMESPACE_NOT_FOUND) {
    return new AppError(
      'MongoDB collection not found. Run the database initialisation script.',
      500,
      'COLLECTION_NOT_FOUND'
    );
  }

  // ── Unknown MongoDB error ──
  return new AppError(
    `MongoDB error: ${err.message}`,
    500,
    'MONGO_ERROR'
  );
};

// ─────────────────────────────────────────────────────────────
// classifyError
// Auto-detects whether an error came from MySQL or MongoDB and
// returns the appropriate AppError.  Falls back gracefully for
// unknown error types.
// ─────────────────────────────────────────────────────────────
const classifyError = (err) => {
  if (err instanceof AppError) return err;

  const isMySQL  = err.sql !== undefined || (err.errno !== undefined && err.sqlMessage !== undefined);
  const isMongo  = err.name?.startsWith('Mongo') || err.code === 11000 || err.code === 11001
                || err.constructor?.name === 'BulkWriteError';

  if (isMySQL) return handleMySQLError(err);
  if (isMongo) return handleMongoError(err);

  // Generic JS / validation errors
  if (err.name === 'ValidationError') {
    return new AppError(err.message, 400, 'VALIDATION_ERROR');
  }
  if (err.name === 'CastError') {
    return new AppError(`Invalid value: ${err.message}`, 400, 'CAST_ERROR');
  }

  return new AppError(err.message || 'An unexpected error occurred.', 500, 'INTERNAL_ERROR');
};

// ─────────────────────────────────────────────────────────────
// asyncHandler
// Wraps an async Express route handler so that any thrown error
// is forwarded to next() automatically.
//
// Usage:
//   router.get('/papers', asyncHandler(async (req, res) => {
//     const papers = await PaperModel.findAll();
//     res.json({ papers });
//   }));
// ─────────────────────────────────────────────────────────────
const asyncHandler = (fn) => (req, res, next) => {
  Promise.resolve(fn(req, res, next)).catch((err) => {
    next(classifyError(err));
  });
};

// ─────────────────────────────────────────────────────────────
// sendError
// Sends a uniform JSON error response.
// In production, stack traces are hidden.
// ─────────────────────────────────────────────────────────────
const sendError = (res, err) => {
  const appErr = err instanceof AppError ? err : classifyError(err);

  const body = {
    error:   appErr.message,
    code:    appErr.code,
    status:  appErr.status,
  };

  if (appErr.isRetryable) {
    body.retryable = true;
  }

  if (process.env.NODE_ENV === 'development') {
    body.stack = appErr.stack;
  }

  return res.status(appErr.status).json(body);
};

module.exports = {
  AppError,
  handleMySQLError,
  handleMongoError,
  classifyError,
  asyncHandler,
  sendError,
};