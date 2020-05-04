# Data definition

This project displays the definition of tables and attributs for APHP.

## Dependencies

The application needs to call `dot` (from graphviz) to be in the path to generate a graph. It is bundled with the docker image.

## Environment variables

The server will connect to a postgresql database and extract its schema with comments from the following variables. Variables are:
- `POSTGRES_DB`: the database
- `POSTGRES_USER`: the DB user
- `POSTGRES_PASSWORD`: the DB password
- `POSTGRES_HOST`: the DB host
- `POSTGRES_PORT`: the DB port
- `AUTH_TOKEN`: the token to allow editing fields. This is mandatory to allow modifying contents

## Run in development

- Run the server: `node server.js` on port `8080`.
- Run the frontend: `npm start`

Open [http://localhost:3000](http://localhost:3000) to view it in the browser.
The page will reload if you make edits.<br />
You will also see any lint errors in the console.

`npm test` launches the test runner in the interactive watch mode. <br/>
Note: no test at the moment....

## Run in production

`npm run build` builds the app for production to the `build` folder.<br />
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.<br />

Then run `node server.js` with the good environment variables, the server will serve the bundle and listen on port `8080`.<br />
A postgres schema with comments was given by APHP as data schema definition.

### Docker

A docker image can import a database schema locally or connect to an existing postgresql database.
- Build the docker image: `docker build -t dataiku/datadef .`
- Run the image (change the exposed port if needed): `docker run -v ${LOCAL_SCHEMA_SQL}:/mnt/schema.sql -p 8080:8080 -d dataiku/datadef` and replace `${LOCAL_SCHEMA_SQL}` by the full path of a schema file
- Run the image to connect to a remote database, and use the `mytoken` string to edit content: `docker run -p 8080:8080 -e AUTH_TOKEN="mytoken" POSTGRES_DB=postgres -e POSTGRES_USER=postgres -e POSTGRES_HOST=172.17.0.1 -e POSTGRES_PASSWORD=password -d dataiku/datadef`