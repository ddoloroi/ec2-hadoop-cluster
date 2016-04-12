H.JobDistribution = H.Class.extend({

    initialize: function (fromFile) {
        this._jobs = {
            time: ['time']
        };
        if (!fromFile) {
            this._startTime = Date.now();
            this._queryInterval = setInterval(H.bind(this._queryJobs, this), 200);
            this._queryJobs();
        }
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
                this._jobs[app.id].deadline = app.deadline;
                this._jobs[app.id].startedTime = app.startedTime;
                this._jobs[app.id].allocatedVCores = [app.id];
                this._jobs[app.id].runningContainers = [app.id];
            }
            this._jobs.time.push(((Date.now() - this._startTime) / 1000).toFixed(1));
            this._jobs[app.id].allocatedVCores.push(Math.max(0, app.allocatedVCores));
            this._jobs[app.id].runningContainers.push(app.runningContainers);
        }
        if (done) {
            clearInterval(this._queryInterval);
            console.log('Job query done');
            this._displayJobs();
        }
    },

    _displayJobs: function () {
        var columns = [this._jobs.time];
        var xs = {};
        for (var key in this._jobs) {
            if (key !== 'time') {
                xs[key] = 'time';
                columns.push(this._jobs[key].allocatedVCores);
            }
        }

        for (var key in this._jobs) {
            // add deadlines later so that we get the nicer colors for the lines
            if (key !== 'time') {
                xs['deadline-' + key] = 'y-deadline-' + key;
                columns.push(['y-deadline-' + key, this._jobs[key].deadline, this._jobs[key].deadline]);
                columns.push(['deadline-' + key, 0, 4]);
            }
        }
        this._chart = c3.generate({
            bindto: '#jobdist-chart',
            data: {
                xs: xs,
                columns: columns
            },
            grid: {
                x: {
                    show: true
                },
                y: {
                    show: true
                }
            },
            point: {show: false},
            transition: {duration: 50}
        });
        var colors = this._chart.data.colors();
        for (var key in this._jobs) {
            if (key !== 'time') {
                colors['deadline-' + key] = colors[key];
            }
        }
        this._chart.load({colors: colors});
    },

    onFilePickerChange: function (e) {
        var reader = new FileReader();
        reader.onload = H.bind(function (e) {
           this._jobs = JSON.parse(e.target.result);
           this._displayJobs()
        }, this);
        reader.readAsText(e.target.files[0]);
    },

    save: function () {
        var blob = new Blob([JSON.stringify(this._jobs, undefined, 4)], {type: 'text/plain'}),
            e = document.createEvent('MouseEvents'),
            a = document.createElement('a');

        a.download = Date.now() + '.json';
        a.href = window.URL.createObjectURL(blob);
        a.dataset.downloadurl =  ['text/csv', a.download, a.href].join(':');
        e.initMouseEvent('click', true, false, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null);
        a.dispatchEvent(e);
    }
});
