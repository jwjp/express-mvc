const assert = require('node:assert/strict');

const request = async (baseUrl, path, options = {}) => {
  const response = await fetch(`${baseUrl}${path}`, options);
  const text = await response.text();
  let body = null;

  try {
    body = JSON.parse(text);
  } catch (err) {
    body = text;
  }

  return { response, body };
};

const withServer = async (app, callback) => {
  const server = app.listen(0);

  try {
    await new Promise((resolve, reject) => {
      server.once('listening', resolve);
      server.once('error', reject);
    });

    const baseUrl = `http://127.0.0.1:${server.address().port}`;
    await callback(baseUrl);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((err) => (err ? reject(err) : resolve()));
    });
  }
};

const clearAppCache = () => {
  delete require.cache[require.resolve('../app')];
};

(async () => {
  clearAppCache();
  process.env.NODE_ENV = 'development';
  delete process.env.CORS_ORIGINS;
  delete process.env.SESSION_SECRET;

  const app = require('../app');
  await withServer(app, async (baseUrl) => {
    const home = await request(baseUrl, '/');
    assert.equal(home.response.status, 200);
    assert.match(home.body, /Welcome to Express/);

    const api = await request(baseUrl, '/api');
    assert.equal(api.response.status, 200);
    assert.equal(api.response.headers.has('x-powered-by'), false);
    assert.equal(api.body.success, true);
    assert.equal(api.body.data.message, 'API is working');

    const invalidLimit = await request(baseUrl, '/users/sample?limit=101');
    assert.equal(invalidLimit.response.status, 400);
    assert.equal(invalidLimit.body.success, false);

    const invalidId = await request(baseUrl, '/users/sample/abc');
    assert.equal(invalidId.response.status, 400);
    assert.equal(invalidId.body.success, false);
  });

  clearAppCache();
  process.env.NODE_ENV = 'production';
  delete process.env.SESSION_SECRET;
  assert.throws(() => require('../app'), /SESSION_SECRET/);

  clearAppCache();
  process.env.NODE_ENV = 'production';
  process.env.SESSION_SECRET = 'test-only-strong-session-secret';
  process.env.CORS_ORIGINS = 'http://allowed.local';

  const productionApp = require('../app');
  await withServer(productionApp, async (baseUrl) => {
    const blockedCors = await request(baseUrl, '/api', {
      headers: { Origin: 'http://blocked.local' }
    });

    assert.equal(blockedCors.response.status, 403);
    assert.equal(blockedCors.body.success, false);
  });

  console.log('Smoke tests passed.');
})().catch((err) => {
  console.error(err);
  process.exit(1);
});
