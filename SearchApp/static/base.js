var change = function () {
    var url = window.location.pathname;        
    $('ul.navbar-nav a[href="' + url + '"]').parent().addClass('active');        
};
change();