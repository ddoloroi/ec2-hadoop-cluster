window.onload = function () {
    H.host = 'http://localhost:8088/ws/v1/cluster';
    new H.UIBinding();
    var chart = c3.generate({
        bindto: '#chart',
        data: {
          columns: [
            ['data1', 30, 200, 100, 400, 150, 250],
            ['data2', 50, 20, 10, 40, 15, 25]
          ]
        }
    });
    console.log('ok');
}
