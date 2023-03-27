[...](../README.md)

# Sparql Endpoint Frontend

- [Sparql Endpoint Frontend](#sparql-endpoint-frontend)
  - [Requirements](#requirements)
  - [Run front end](#run-front-end)
    - [`npm install`](#npm-install)
    - [`npm start`](#npm-start)
    - [`npm run build`](#npm-run-build)
    - [`npm run validate`](#npm-run-validate)
    - [`npm run format`](#npm-run-format)

## Requirements

We recommend using `nvm`, a `.nvmrc` file is available in the root folder of the frontend project.

- node 16
- npm 8

## Run front end

Before using npm, we recommend using `nvm` to manage your `node` and `npm` versions.

- If you want/can use `nvm`, just type `nvm use` from the root folder of the front-end project
- If you **don't** want to / cannot use `nvm`, use the version described in the `.nvmrc` file

In the project directory, you can run:

### `npm install`

You may have to run it like this: `npm i --legacy-peer-deps`

If you run into issues during compilation on Macos, try with Node 18 (`nvm use 18`)

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:3000](http://localhost:3000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `npm run build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `npm run validate`

Checks the code formatting with `npm run validate`. For pull requests, the validate script shouldn't return an error.

### `npm run format`

Format your code with `npm run format`.
