var last4_file;
function initDoc(report_archive_default, last4days) {
    last4_file = last4days;
    requestDoc('main', report_archive_default);
    requestDoc('sidebarContents',last4days);
}

function searchArchive() {
    var searchterm = document.getElementById('archiveSearch').value;
    if (searchterm != ''){
    var reqdata = document.getElementById('hiddenReqData').innerHTML;
    
    var reqnums = reqdata.split(',');
   
    var curItems= 0; 
    var maxItems = 20;
    var searchresults = "<h3>Search Results</h3>\n<div id='sidebarResults'>";
    for (var i=0; i < reqnums.length;i++) {
        if (reqnums[i].search(searchterm) != -1) {
            if (curItems < maxItems) {
                searchresults += "<button onclick=\"requestDoc('main','/static/reports/" + reqnums[i] + "/report_" + reqnums[i] +".html')\">Report " + reqnums[i] + "</button>";
                curItems++;
            }
        }
    }
    searchresults += "</div>"
    document.getElementById('sidebarContents').innerHTML = searchresults;
    }
    else {
        requestDoc('sidebarContents',last4_file);
    }
}

function ToggleSidebar() {
    var x = document.getElementById("sidebar");
    if (x.style.display === "none") {
        x.style.display = "flex";
    } else {
        x.style.display = "none";
    }
}

function requestDoc(elementId, page) {
    var xhttp = new XMLHttpRequest();
    xhttp.onreadystatechange = function() {
        if (this.readyState == 4 && this.status == 200) {
            document.getElementById(elementId).innerHTML = 
            this.responseText;
        }
    };
    xhttp.open("GET", page, true);
    xhttp.send();
}


