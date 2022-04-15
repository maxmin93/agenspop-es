# AgensBrowser-web: Frontend

Angular8 + Bootstrap(v4.3.1) + Cytoscape.js(v3.12.1) + El Grapho(v2.4.0)

## Setup

* Ensure you have Node 10.15.1+ and NPM 6.13.7+ installed.
* Install Angular CLI `npm i -g angular-cli@^6.1.3`
* Install Typescript 2.0 `npm i -g typescript`
* Install TSLint `npm install -g tslint`
* Install Protractor for e2e testing `npm install -g protractor`
* Install Node packages `npm i`
* Update Webdriver `webdriver-manager update` and `./node_modules/.bin/webdriver-manager update`
* Run local build `ng serve`

## Dev mode

vi src/app/services/ap-api.service.ts
```
  apiUrl = `http://localhost:8080`;
```

Then, you can work frontend locally.

## Running the app

ng serve

open browser ==> http://localhost:8082
