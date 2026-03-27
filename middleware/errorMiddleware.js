/**
 * middleware/errorMiddleware.js
 * 
 * Express global error-handling middleware.
 * Must be registered AFTER all routes in server.js:
 *   app.use(errorMiddleware);
 */

const { AppError, classifyError } = require('../utils/errorHandler');

/**
 * Global error handler.
 * Converts any error into a consistent JSON response.
 */
const errorMiddleware = (err, req, res, next) => {
  // Convert to AppError if it isn't one already
  const appErr = err instanceof AppError ? err : classifyError(err);

  // Log server-side errors (5xx) — don't log client errors (4xx) as errors
  if (appErr.status >= 500) {
    console.error(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl} — ${appErr.status} ${appErr.code}: ${appErr.message}`);
    if (process.env.NODE_ENV === 'development') {
      console.error(appErr.stack);
    }
  } else {
    console.warn(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl} — ${appErr.status} ${appErr.code}: ${appErr.message}`);
  }

  const body = {
    error:  appErr.message,
    code:   appErr.code,
    status: appErr.status,
  };

  if (appErr.isRetryable) {
    body.retryable = true;
  }

  // Only expose stack trace in development
  if (process.env.NODE_ENV === 'development') {
    body.stack = appErr.stack;
  }

  return res.status(appErr.status).json(body);
};

/**
 * 404 handler — for routes that don't match any registered route.
 * Register this BEFORE the error handler but AFTER all routes.
 */
const notFoundMiddleware = (req, res, next) => {
  next(new AppError(`Route '${req.method} ${req.originalUrl}' not found.`, 404, 'ROUTE_NOT_FOUND'));
};

module.exports = { errorMiddleware, notFoundMiddleware };