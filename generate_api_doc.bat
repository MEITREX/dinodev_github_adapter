@echo off
echo Generating API documentation...
echo This requires npm to be installed!

REM clear old docs
del api.md
REM Delete the combined file
del combined.graphql

set title=Scrum Game API

REM install graphql-markdown if not installed
if not exist node_modules\graphql-markdown (
   echo Installing graphql-markdown, run this script again after installation
   npm install graphql-markdown
)

REM Concatenate all graphql files into one
for /r src\main\resources\graphql %%i in (*.graphqls) do type "%%i" >> combined.graphql

REM Run npx in a separate cmd instance
cmd /c npx graphql-markdown combined.graphql --title "%title%" > api.md

REM wait for user input
echo API documentation generated.

REM Delete the combined file
del combined.graphql
