// errorMiddleware converts backend errors and unknown routes into HTTP responses.
const { AppError, classifyError } = require('../utils/errorHandler');





const errorMiddleware = (err, req, res, next) => {
  
  const appErr = err instanceof AppError ? err : classifyError(err);

  
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

  
  if (process.env.NODE_ENV === 'development') {
    body.stack = appErr.stack;
  }

  return res.status(appErr.status).json(body);
};





const notFoundMiddleware = (req, res, next) => {
  next(new AppError(`Route '${req.method} ${req.originalUrl}' not found.`, 404, 'ROUTE_NOT_FOUND'));
};

module.exports = { errorMiddleware, notFoundMiddleware };
