const { validationResult } = require('express-validator');

const validate = (rules) => [
  ...rules,
  (req, res, next) => {
    const errors = validationResult(req);

    if (!errors.isEmpty()) {
      return res.error('Validation failed.', 400, errors.array());
    }

    return next();
  }
];

module.exports = { validate };
