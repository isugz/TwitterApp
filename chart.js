<script>
   var ctx = document.getElementById("chart");
   console.log("here")
   var myChart = new Chart(ctx, {
    	type: 'horizontalBar',
    	data: {
        	labels: [{% for item in labels %}
                  	"{{item}}",
                 	{% endfor %}],
        	datasets: [{
            	label: '# of Mentions',
            	data: [{% for item in values %}
     	                 {{item}},
                    	{% endfor %}],
            	backgroundColor: [
                	'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
          	        'rgba(75, 192, 192, 0.2)',
                	'rgba(153, 102, 255, 0.2)',
                	'rgba(255, 159, 64, 0.2)',
                	'rgba(255, 99, 132, 0.2)',
                	'rgba(54, 162, 235, 0.2)',
                	'rgba(255, 206, 86, 0.2)',
                	'rgba(75, 192, 192, 0.2)',
                	'rgba(153, 102, 255, 0.2)'
            	],
            	borderColor: [
                	'rgba(255,99,132,1)',
                	'rgba(54, 162, 235, 1)',
        	        'rgba(255, 206, 86, 1)',
                	'rgba(75, 192, 192, 1)',
                	'rgba(153, 102, 255, 1)',
                	'rgba(255, 159, 64, 1)',
                	'rgba(255,99,132,1)',
                	'rgba(54, 162, 235, 1)',
                	'rgba(255, 206, 86, 1)',
                	'rgba(75, 192, 192, 1)',
                	'rgba(153, 102, 255, 1)'
            	],
            	borderWidth: 1
        	}]
    	},
    	options: {
        	scales: {
	            yAxes: [{
                	ticks: {
                    	beginAtZero:true
                	}
            	}]
        	}
    	}
   });
   var src_Labels = [];
   var src_Data = [];
   setInterval(function(){
    	$.getJSON('/refreshData', {
    	}, function(data) {
        	src_Labels = data.sLabel;
        	src_Data = data.sData;
    	});
    	myChart.data.labels = src_Labels;
    	myChart.data.datasets[0].data = src_Data;
    	myChart.update();
   },1000);
</script>