1. You'll need a DESDM database account. To request an account go <a "href=https://opensource.ncsa.illinois.edu/confluence/display/DESDM/Data+Access+FAQ#DataAccessFAQ-Q:HowcanIgetaccesstotheDESDMdatabase?"> here.</a> 
2. Once you have an account, you'll need to create a credential file, called .desservices.ini, typically in your home directory. If you have easyaccess, it will create one for you. Info on easyaccess is also at the link above.
3. Run the docker container: 

docker run -p 5000:5000 -v <local location to your services file>:/desdm-dash/.desservices.ini -v <local location to where you want processing reports stored>:/desdm-dash/app/static/reports/ -d desdmdash:<version>
