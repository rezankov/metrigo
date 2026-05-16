FROM node:22-slim

WORKDIR /app

COPY package.json ./
RUN npm install
RUN npm install react-chartjs-2 chart.js

COPY . .

RUN npm run build

EXPOSE 3000

CMD ["npm", "run", "start"]