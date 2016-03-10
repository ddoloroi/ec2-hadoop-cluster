H.JobDistribution = H.Class.extend({

    initialize: function () {
        this._jobs = {};
        this._chart = c3.generate({
            bindto: '#jobdist-chart',
            data: {
                columns: []
            }
        });
        this._startTime = 10000;
        //this._queryInterval = setInterval(H.bind(this._queryJobs, this), 1000);
        this._queryJobs();
    },

    _queryJobs: function () {
        $.ajax({
            dataType: "json",
            url: H.host + '/apps',
            data: {startedTimeBegin: this._startTime},
            success: H.bind(this._handleJobQuery, this)
        });
    },

    _handleJobQuery: function (data) {
        console.log(data);
        var apps = data.apps.app;
        var done = apps.length > 0;
        for (var i = 0; i < done.length; i++) {
            var app = apps[i];
            done = done && app.state === 'FINISHED';
            if (this._jobs[app.id] === undefined) {
                this._jobs[app.id] = {};
                this._jobs[app.id].name = app.name;
                this._jobs[app.id].startedTime = app.startedTime;
                this._jobs[app.id].data = [];
            }
            this._jobs[app.id].data.push({
                time: (Date.now() - this._startTime) / 100,
                allocatedVCores: app.allocatedVCores,
                runningContainers: app.runningContainers
            });
        }
        if (done) {
            clearInterval(this._queryInterval);
            console.log('Job query done');
        }
    }
});
