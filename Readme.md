This image runs a web dashboard for Dark Energy Survey Data Management that tracks current processing. To run:

1. You'll need a DESDM database account. To request an account go [here](https://opensource.ncsa.illinois.edu/confluence/display/DESDM/Data+Access+FAQ#DataAccessFAQ-Q:HowcanIgetaccesstotheDESDMdatabase?).
2. Once you have an account, you'll need to create a credential file, called .desservices.ini, typically in your home directory. If you have easyaccess, it will create one for you. Info on easyaccess is also at the link above.
3. Run the docker container: 

docker run -p 5000:5000 -v local/path/to/your/services/file:/desdm-dash/.desservices.ini -v local/path/to/store/reports:/desdm-dash/app/desdm-dash-static/reports/ -d desdmdash:version
