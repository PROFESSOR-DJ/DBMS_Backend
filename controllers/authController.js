// authController handles backend login, registration, and account actions.
const bcrypt    = require('bcryptjs');
const jwt       = require('jsonwebtoken');
const UserModel = require('../models/mysql/userModel');
const { validationResult } = require('express-validator');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');




const register = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }

  const { name, email, password } = req.body;

  
  const existingUser = await UserModel.findByEmail(email.toLowerCase());
  if (existingUser) {
    throw new AppError('An account with this email already exists.', 409, 'DUPLICATE_ENTRY');
  }

  
  const salt          = await bcrypt.genSalt(10);
  const password_hash = await bcrypt.hash(password, salt);

  let result;
  try {
    result = await UserModel.create({
      name,
      email:    email.toLowerCase(),
      password: password_hash,
      role:     'researcher',
    });
  } catch (err) {
    const appErr = classifyError(err);
    
    if (appErr.code === 'DUPLICATE_ENTRY') {
      throw new AppError('An account with this email already exists.', 409, 'DUPLICATE_ENTRY');
    }
    throw appErr;
  }

  const user_id = result.insertId;

  const token = jwt.sign(
    { user_id, email: email.toLowerCase() },
    process.env.JWT_SECRET,
    { expiresIn: process.env.JWT_EXPIRE }
  );

  res.status(201).json({
    message: 'User registered successfully.',
    token,
    user: { user_id, name, email: email.toLowerCase() },
  });
});




const login = asyncHandler(async (req, res) => {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({ errors: errors.array() });
  }

  const { email, password } = req.body;

  const user = await UserModel.findByEmail(email.toLowerCase());
  if (!user) {
    
    
    throw new AppError('Invalid email or password.', 401, 'INVALID_CREDENTIALS');
  }

  const isPasswordValid = await bcrypt.compare(password, user.password);
  if (!isPasswordValid) {
    throw new AppError('Invalid email or password.', 401, 'INVALID_CREDENTIALS');
  }

  const token = jwt.sign(
    { user_id: user.user_id, email: user.email },
    process.env.JWT_SECRET,
    { expiresIn: process.env.JWT_EXPIRE }
  );

  await UserModel.updateLastLogin(user.user_id);

  res.json({
    message: 'Login successful.',
    token,
    user: {
      user_id:  user.user_id,
      name:     user.name,
      email:    user.email,
      role:     user.role,
    },
  });
});




const forgotPassword = asyncHandler(async (req, res) => {
  const { email } = req.body;

  if (!email || email.trim().length === 0) {
    throw new AppError('Email is required.', 400, 'MISSING_FIELDS');
  }

  
  await UserModel.findByEmail(email.toLowerCase());  

  res.json({
    message: 'If an account exists with this email, you will receive password reset instructions.',
    note:    'This is a simulation for the DBMS project. No actual email is sent.',
  });
});




const authenticate = (req, res, next) => {
  const authHeader = req.headers['authorization'];
  const token      = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({ error: 'Access token required.', code: 'NO_TOKEN' });
  }

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) {
      const code    = err.name === 'TokenExpiredError' ? 'TOKEN_EXPIRED' : 'INVALID_TOKEN';
      const message = err.name === 'TokenExpiredError' ? 'Token has expired. Please log in again.' : 'Invalid token.';
      return res.status(403).json({ error: message, code });
    }
    req.user = user;
    next();
  });
};




const getProfile = asyncHandler(async (req, res) => {
  const user = await UserModel.findById(req.user.user_id);
  if (!user) {
    throw new AppError('User not found.', 404, 'NOT_FOUND');
  }
  res.json({
    user_id:    user.user_id,
    name:       user.name,
    email:      user.email,
    role:       user.role || 'researcher',
    created_at: user.created_at,
    last_login: user.last_login || null,
  });
});




const updateProfile = asyncHandler(async (req, res) => {
  const { name, email } = req.body;

  if (!name && !email) {
    throw new AppError('Provide at least one field to update (name or email).', 400, 'MISSING_FIELDS');
  }
  const updates = {};

  if (name !== undefined) updates.name = String(name).trim();
  if (email !== undefined) updates.email = String(email).trim().toLowerCase();

  if (updates.name !== undefined && updates.name.length === 0) {
    throw new AppError('Name cannot be empty.', 400, 'INVALID_PARAM');
  }

  if (updates.email !== undefined) {
    if (updates.email.length === 0) {
      throw new AppError('Email cannot be empty.', 400, 'INVALID_PARAM');
    }

    const existingUser = await UserModel.findByEmail(updates.email);
    if (existingUser && existingUser.user_id !== req.user.user_id) {
      throw new AppError('An account with this email already exists.', 409, 'DUPLICATE_ENTRY');
    }
  }

  await UserModel.updateProfile(req.user.user_id, updates);
  const user = await UserModel.findById(req.user.user_id);

  res.json({
    message: 'Profile updated successfully.',
    user,
  });
});

module.exports = {
  register,
  login,
  forgotPassword,
  authenticate,
  getProfile,
  updateProfile,
};
