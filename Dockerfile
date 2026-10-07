# Use lightweight Node.js LTS image
FROM node:22-alpine

# Set working directory and ensure write permissions for the node user
WORKDIR /app
RUN mkdir -p /app/output && chown -R node:node /app

# Install dependencies with correct ownership
COPY --chown=node:node package*.json ./
RUN npm ci

# Copy application source code with correct ownership
COPY --chown=node:node . .

# Ensure non-root execution for security
USER node

# Default command: runs the consumer
# Can be overridden via CLI:
#   docker run <image> npm start           (Producer)
#   docker run <image> npm test            (Unit tests)
#   docker run <image> npm run demo:sim    (In-memory demo)
CMD ["node", "src/consumer.js"]
