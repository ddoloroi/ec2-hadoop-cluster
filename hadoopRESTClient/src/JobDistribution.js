H.JobDistribution = H.Class.extend({

    initialize: function () {
        this._jobs = {};
        this._chart = c3.generate({
            bindto: '#jobdist-chart',
            data: {
                x: 'time',
                columns: []
            }
        });
        this._startTime = Date.now();
        this._queryInterval = setInterval(H.bind(this._queryJobs, this), 200);
        this._queryJobs();
    },

    _queryJobs: function () {
        $.ajax({
            dataType: "json",
            url: H.resourceManagerHost + '/apps',
            data: {startedTimeBegin: this._startTime},
            success: H.bind(this._handleJobQuery, this)
        });
    },

    _handleJobQuery: function (data) {
        if (!data.apps) {
            return;
        }
        var apps = data.apps.app;
        var done = apps.length > 0;
        for (var i = 0; i < apps.length; i++) {
            var app = apps[i];
            done = done && app.state === 'FINISHED';
            if (this._jobs[app.id] === undefined) {
                this._jobs[app.id] = {};
                this._jobs[app.id].name = app.name;
                this._jobs[app.id].startedTime = app.startedTime;
                this._jobs[app.id].time = ['time'];
                this._jobs[app.id].allocatedVCores = [app.id];
                this._jobs[app.id].runningContainers = [app.id];
            }
            this._jobs[app.id].time.push((Date.now() - this._startTime) / 100);
            this._jobs[app.id].allocatedVCores.push(Math.max(0, app.allocatedVCores));
            this._jobs[app.id].runningContainers.push(app.runningContainers);

            this._chart.load({
                columns: [this._jobs[app.id].time, this._jobs[app.id].allocatedVCores]
            });
        }
        if (done) {
            clearInterval(this._queryInterval);
            console.log('Job query done');
        }
    }
});
