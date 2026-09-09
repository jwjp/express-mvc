# Express MVC 🚀

A robust MVC-style boilerplate for Express.js applications with built-in security, logging, and performance optimizations.

## 🌟 Features

- **MVC Architecture**
    - Clear separation of concerns with Models, Views, and Controllers
    - Dynamic controller loading system
    - Static file serving for views

- **Security First 🔒**
    - Helmet.js integration
    - Configurable CORS allowlist
    - Rate limiting
    - Speed limiting
    - Session management with file store
    - Request payload size limits

- **Advanced Logging 📝**
    - Winston logger with daily rotate
    - Morgan HTTP request logging
    - Separate log files for different environments

- **Performance Optimized ⚡**
    - Express 5.1.0
    - Built-in rate limiting
    - Speed limiting for DDoS protection
    - Static file serving

## 📁 Project Structure

```
express-mvc/
├── bin/
│   └── www           # Application entry point
├── controller/       # Route controllers
├── logs/             # Application logs
├── middleware/       # Custom middleware
├── model/            # Data models
├── modules/          # Reusable modules
├── sessions/         # Session storage
└── view/             # Static files
    ├── images/
    ├── javascripts/
    └── stylesheets/
```


## 🛠 Prerequisites

- Node.js >= 14.x
- PM2 (for production deployment)
- MySQL (optional)

## 📦 Installation

```shell script
# Clone the repository
git clone https://github.com/jiwonio/express-mvc.git

# Install dependencies
npm install

# Create .env file
cp .env.example .env
```


## 🚀 Running the Application

### Development Mode
```shell script
pm2 start ecosystem.config.js --only express-mvc/development
```


### Production Mode
```shell script
pm2 start ecosystem.config.js --only express-mvc/production
```


## 🔧 Configuration

The application can be configured through environment variables in `.env`:

```
DB_HOST=localhost
DB_NAME=sample
DB_USERNAME=root
DB_PASSWORD=root
SESSION_SECRET=change-me-to-a-long-random-secret
CORS_ORIGINS=http://localhost:3000
TRUST_PROXY=
RATE_LIMIT=100
SLOW_DOWN_AFTER=50
SLOW_DOWN_DELAY_MS=2000
JSON_LIMIT=1mb
FORM_LIMIT=1mb
ALLOWED_IPS=
```


## 🔍 Optional Enhancements

The following features are prepared but optional:

- HTTPS support via Nginx reverse proxy
- Passport.js authentication
- Caching layer
- Custom authentication/authorization middleware

## 🛡 Security Features

- Helmet.js for security headers
- Rate limiting: 100 requests per 15 minutes
- Speed limiting: Delay after 50 requests
- Session management with FileStore
- CORS allowlist via `CORS_ORIGINS`
- JSON and form payload size limits

In production, set `SESSION_SECRET` to a strong non-default value. If the app runs behind a reverse proxy that terminates HTTPS, set `TRUST_PROXY` to the proxy hop count, for example `1`.
`ALLOWED_IPS` is optional and should contain only trusted IPs that may bypass authentication.

## 📝 Logging

Logs are automatically rotated daily and stored in the `logs` directory:
- Access logs
- Error logs
- Application logs

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the UNLICENSE - see the LICENSE file for details.
