window.onload = function () {
    H.resourceManagerHost = 'http://localhost:8088/ws/v1/cluster';
    H.historyServerHost = 'http://localhost:19888/ws/v1/history';
    new H.UIBinding();
    console.log('Initialized');
}
