$(document).ready(function() {
    $('#example').DataTable( {
        "fixedHeader": true,
        "lengthMenu": [ [10, 50, 100, 500, -1], [10, 50,100,500, "All"] ],
        "pageLength": -1
    } );
} );


