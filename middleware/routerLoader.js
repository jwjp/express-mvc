// middleware/routerLoader.js
const path = require('path');
const fs = require('fs');
const { logger } = require('../modules/logger');

// Route registration function is isolated into a separate module
const registerRoute = (app, fullPath, cleanRoute) => {
  try {
    const routeModule = require(fullPath);
    const routeHandler = routeModule.router || routeModule;

    if (typeof routeHandler !== 'function') {
      throw new TypeError(`Route module must export a router or middleware function: ${fullPath}`);
    }

    app.use(cleanRoute, routeHandler);
    logger.info(`Route registered: ${cleanRoute} (${fullPath})`);
  } catch (err) {
    logger.error(`Error loading route ${fullPath}: ${err.message}`);
    throw err;
  }
};

// Higher-order function expressed as an arrow function
const routerLoader = (controllerPath) => (app) => {
  const processDirectory = (dir, baseRoute = '') => {
    fs.readdirSync(dir).forEach(file => {
      const fullPath = path.join(dir, file);

      if (fs.statSync(fullPath).isDirectory()) {
        processDirectory(fullPath, `${baseRoute}/${file}`);
        return;
      }

      if (!file.endsWith('.js')) return;

      const route = file === 'index.js' ? baseRoute : `${baseRoute}/${path.basename(file, '.js')}`;
      const cleanRoute = route.replace(/\/+/g, '/').replace(/^\/$/, '') || '/';

      registerRoute(app, fullPath, cleanRoute);
    });
  };

  processDirectory(controllerPath);
  return app;
};

module.exports = routerLoader;
