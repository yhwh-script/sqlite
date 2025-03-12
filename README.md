# Description
A [yhwh-script](https://github.com/yhwh-script) project with [SQLite WASM](https://www.npmjs.com/package/@sqlite.org/sqlite-wasm) built on <a title="Vite" href="https://vitejs.dev"><img height="20" alt="Vitejs-logo" src="https://vitejs.dev/logo.svg"></a> without examples.

# Quick Start
Just call `npm install` and `npm run dev`.

# FYI
You can create Single File Web-Components in dedicated `.html` files inside the `public/elements` folder. `npm run dev` and `npm run build` will generate maps in `src/elements/index.js` which are being fetched in the `/src/main.js` script for defining your CustomElements.

Please understand that the project structure is leaned on [valid-custom-element-names](https://html.spec.whatwg.org/multipage/custom-elements.html#valid-custom-element-name), i.e. using a dash ("-") is mandatory.

In `/src/publicApis/index.js` you can define APIs for your custom-elements (for example to the SQLite module) via the window object.

Just stick to the examples and you are good to go. Have fun!