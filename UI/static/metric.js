$(document).ready(function() {
    $('#Input1').on('change', function() {
      var selectedOption = $(this).val();
      $.ajax({
        url: '/handle_topmetrics',
        type: 'POST',
        data: { mySelect: selectedOption },
        success: function(response) {
          $('#metricresults').html(response);
        }
      });
    });
  });