# agenspop-es-front

This project was generated with [Angular CLI](https://github.com/angular/angular-cli) version 8.211.


## Development server

Run `ng serve --host 0.0.0.0 --port 8082` for a dev server. <br>
And open browser to `http://localhost:8082/`.


## Issues

[\[BABEL\] Could not find plugin "proposal-class-properties"](https://github.com/babel/babel/issues/12247)

* reinstall `@babel/preset-env` ^7.12.11 at "devDependencies"
  * ^7.8.7 에서 해결되었다는데, 그래도 문제 발생
* edit `package.json`
* run `npx npm-force-resolutions`
* re-run `npm install`

```json
{
    "scripts": {
        "preinstall": "npx npm-force-resolutions",
        ...
    },
    ...
    "resolutions": {
        "@babel/preset-env": "7.12.11"
    }
}
```
