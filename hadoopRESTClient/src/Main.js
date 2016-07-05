window.onload = function () {
    H.resourceManagerHost = 'http://ec2-54-93-94-219.eu-central-1.compute.amazonaws.com:8088/ws/v1/cluster';
    H.historyServerHost = 'http://ec2-54-93-94-219.eu-central-1.compute.amazonaws.com:19888/ws/v1/history';
    new H.UIBinding();
    console.log('Initialized');
}
