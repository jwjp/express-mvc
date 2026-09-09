const express = require('express');
const { body, param, query, matchedData } = require('express-validator');
const router = express.Router();
const { getFooBar, getTotalCount, createFoobar } = require('../model/users');
const { validate } = require('../middleware/validate');

/**
 * GET /
 */
router.get('/', function(req, res, next) {
  res.send('respond with a resource');
});

/**
 * GET /users/sample
 */
router.get('/sample', validate([
  query('page').optional().isInt({ min: 1 }).toInt(),
  query('limit').optional().isInt({ min: 1, max: 100 }).toInt()
]), async (req, res, next) => {
  try {
    const { page = 1, limit = 10 } = matchedData(req, { locations: ['query'] });
    const offset = (page - 1) * limit;

    // 함수를 직접 호출
    const [users, total] = await Promise.all([
      getFooBar({ limit, offset }),
      getTotalCount()
    ]);

    res.success('success', {
      users,
      pagination: {
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit)
      }
    });
  } catch (error) {
    next(error);
  }
});

/**
 * GET /users/sample/:id
 */
router.get('/sample/:id', validate([
  param('id').isInt({ min: 1 }).toInt()
]), async (req, res, next) => {
  try {
    const { id: userId } = matchedData(req, { locations: ['params'] });

    const users = await getFooBar({
      limit: 1,
      offset: 0,
      userId
    });

    if (!users || users.length === 0) {
      return res.status(404).json({
        success: false,
        message: 'Not found.'
      });
    }

    res.success('success', users[0]);
  } catch (error) {
    next(error);
  }
});

/**
 * POST /users/sample
 */
router.post('/sample', validate([
  body('message').trim().isLength({ min: 1, max: 255 }),
  body('comment').trim().isLength({ min: 1, max: 255 })
]), async (req, res, next) => {
  try {
    const { message, comment } = matchedData(req, { locations: ['body'] });
    const result = await createFoobar(
      { message },
      { comment }
    );
    res.success('success', result);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
