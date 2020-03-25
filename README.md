# Data definition

This project displays the definition of tables and attributs for APHP.

## Run in development

- Run the server: `node server.js tables.csv attributes.csv` on port `8080`. `tables.csv` and `attributes.csv` are given by APHP as data schema definition
- Run the frontend: `npm start`

Open [http://localhost:3000](http://localhost:3000) to view it in the browser.
The page will reload if you make edits.<br />
You will also see any lint errors in the console.

`npm test` launches the test runner in the interactive watch mode.

## Run in production 

`npm run build` builds the app for production to the `build` folder.<br />
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.<br />

Then run `node server.js tables.csv attributes.csv`, the server will serve the bundle and listen on port `8080`.<br />
`tables.csv` and `attributes.csv` are given by APHP as data schema definition.

### Docker

Because the server needs 2 csv files: tables definition and attributes definition, we need to mount a volume with thoe files on disk.
- Build the docker image: docker build -t dataiku/datadef .
- Run the image (change the exposed port if needed): `docker run -p 8080:8080 -v ${local-tables-csv}:/mnt/tables.csv -v ${local-attributes-csv}:/mnt/attributes.csv -d dataiku/datadef` where `${local-tables-csv}` and `${local-attributes-csv}` are path to the local csv files. 
