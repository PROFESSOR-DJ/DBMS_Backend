// authorController handles backend author CRUD and search requests.
const AuthorModel = require('../models/mysql/authorModel');
const { AppError, classifyError, asyncHandler } = require('../utils/errorHandler');




const getAllAuthors = asyncHandler(async (req, res) => {
  const limit  = parseInt(req.query.limit,  10) || 100;
  const offset = parseInt(req.query.offset, 10) || 0;

  if (isNaN(limit) || isNaN(offset) || limit < 1 || offset < 0) {
    throw new AppError('Invalid pagination parameters.', 400, 'INVALID_PARAM');
  }

  const authors = await AuthorModel.findAll(limit, offset);
  res.json({ authors, count: authors.length, limit, offset });
});




const searchAuthors = asyncHandler(async (req, res) => {
  const { q } = req.query;
  if (!q || q.trim().length === 0) {
    throw new AppError('Query parameter "q" is required.', 400, 'MISSING_PARAM');
  }

  const authors = await AuthorModel.searchByName(q.trim());
  res.json({ authors, count: authors.length, query: q });
});




const createAuthor = asyncHandler(async (req, res) => {
  const { name, paper_id } = req.body;

  if (!name || name.trim().length === 0) {
    throw new AppError('Author name is required.', 400, 'MISSING_FIELDS');
  }
  if (!paper_id) {
    throw new AppError('Linking to a paper (paper_id) is required for new authors.', 400, 'MISSING_FIELDS');
  }

  let result;
  try {
    result = await AuthorModel.create({ author_name: name.trim(), paper_id });
  } catch (err) {
    const appErr = classifyError(err);

    
    if (appErr.code === 'DUPLICATE_ENTRY') {
      throw new AppError(
        `Author '${name}' is already linked to paper '${paper_id}'.`,
        409,
        'DUPLICATE_ENTRY'
      );
    }

    
    if (appErr.code === 'FK_REFERENCE_NOT_FOUND') {
      throw new AppError(
        `Paper '${paper_id}' does not exist. Create the paper before linking an author.`,
        400,
        'FK_REFERENCE_NOT_FOUND'
      );
    }

    
    if (appErr.code === 'BUSINESS_RULE_VIOLATION') {
      throw new AppError(appErr.message, 409, 'BUSINESS_RULE_VIOLATION');
    }

    throw appErr;
  }

  res.status(201).json({
    message:   'Author created and linked to paper.',
    author_id: result.insertId,
    name:      name.trim(),
    paper_id,
  });
});




const updateAuthor = asyncHandler(async (req, res) => {
  const { id }  = req.params;
  const { name } = req.body;

  if (!name || name.trim().length === 0) {
    throw new AppError('Author name is required.', 400, 'MISSING_FIELDS');
  }

  
  const existing = await AuthorModel.findById(id);
  if (!existing) {
    throw new AppError(`Author with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  try {
    await AuthorModel.update(id, { author_name: name.trim() });
  } catch (err) {
    const appErr = classifyError(err);
    
    if (appErr.code === 'DUPLICATE_ENTRY') {
      throw new AppError(
        `Another author named '${name}' already exists.`,
        409,
        'DUPLICATE_ENTRY'
      );
    }
    throw appErr;
  }

  res.json({ message: 'Author updated.', author_id: id, name: name.trim() });
});






const deleteAuthor = asyncHandler(async (req, res) => {
  const { id } = req.params;

  
  const existing = await AuthorModel.findById(id);
  if (!existing) {
    throw new AppError(`Author with id '${id}' not found.`, 404, 'NOT_FOUND');
  }

  try {
    await AuthorModel.delete(id);
  } catch (err) {
    const appErr = classifyError(err);

    
    if (appErr.code === 'BUSINESS_RULE_VIOLATION') {
      throw new AppError(appErr.message, 409, 'AUTHOR_HAS_PAPERS');
    }

    
    if (appErr.code === 'FK_REFERENCE_EXISTS') {
      throw new AppError(
        `Author '${existing.author_name}' is still linked to papers. Remove the paper links first.`,
        409,
        'FK_REFERENCE_EXISTS'
      );
    }

    throw appErr;
  }

  res.json({ message: 'Author deleted.', author_id: id });
});

module.exports = {
  getAllAuthors,
  searchAuthors,
  createAuthor,
  updateAuthor,
  deleteAuthor,
};
