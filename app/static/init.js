//$(document).ready(function() {
$(window).load(function() {
    $('#example').DataTable( {
        "fixedHeader": {
            header: true,
            footer: false,
            },
        "lengthMenu": [ [10, 50, 100, 500, -1], [10, 50,100,500, "All"] ],
        "pageLength": -1
    } );
} );



