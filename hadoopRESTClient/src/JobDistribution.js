H.JobDistribution = H.Class.extend({

    initialize: function (fromFile) {
        this._jobs = {};
        this._maxResources = 0;
        this._maxTime = 15;
        if (!fromFile) {
            this._startTime = Date.now();
            this._queryInterval = setInterval(H.bind(this._queryJobs, this), 200);
            this._displayInterval = setInterval(H.bind(this._displayJobs, this), 3000);
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
        var now = ((Date.now() - this._startTime) / 1000).toFixed(1);
        for (var i = 0; i < apps.length; i++) {
            var app = apps[i];
            done = done && app.state === 'FINISHED';
            if (app.state == 'FINISHED') {
                continue;
            }
            if (this._jobs[app.id] === undefined) {
                this._jobs[app.id] = {};
                this._jobs[app.id].name = app.name;
                this._jobs[app.id].deadline = ((app.startedTime - this._startTime) / 1000 + app.deadline).toFixed(1);
                this._jobs[app.id].startedTime = app.startedTime;
                this._jobs[app.id].allocatedVCores = [app.id];
                this._jobs[app.id].runningContainers = [app.id];
                this._jobs[app.id].time = ['time-' + app.id];
                this._maxTime = Math.max(this._maxTime, this._jobs[app.id].deadline);
            }
            this._jobs[app.id].allocatedVCores.push(Math.max(0, app.allocatedVCores));
            this._jobs[app.id].runningContainers.push(app.runningContainers);
            this._jobs[app.id].time.push(now);
            this._maxResources = Math.max(this._maxResources, app.allocatedVCores);
            this._maxTime = Math.max(this._maxTime, now);
        }
        if (done) {
            clearInterval(this._queryInterval);
            clearInterval(this._displayInterval);
            console.log('Job query done');
            this._displayJobs();
        }
    },

    _displayJobs: function () {
        if (this._chart) {
            this._chart.destroy();
        }
        var columns = [];
        var xs = {};
        var count = 1;
        var names = {};
        for (var id in this._jobs) {
            xs[id] = 'time-' + id;
            columns.push(this._jobs[id].allocatedVCores);
            columns.push(this._jobs[id].time);
            names[id] = 'Application ' + count;
            names['deadline-' + id] = 'Deadline ' + count;
            count += 1;
        }

        for (var id in this._jobs) {
            // add deadlines later so that we get the nicer colors for the lines
            xs['deadline-' + id] = 'y-deadline-' + id;
            columns.push(['y-deadline-' + id, this._jobs[id].deadline, this._jobs[id].deadline]);
            columns.push(['deadline-' + id, 0, this._maxResources]);
        }
        values = [];
        for (var i = 0; i < this._maxTime; i += parseInt(this._maxTime / 15)) {
            values.push(i);
        }
        this._chart = c3.generate({
            bindto: '#jobdist-chart',
            data: {
                xs: xs,
                columns: columns,
                names: names
            },
            grid: {
                x: {
                    show: true
                },
                y: {
                    show: true
                }
            },
            axis: {
                x: {
                    label: {
                        text: 'Time (s)',
                        position: 'outer-center'
                    },
                    tick: {
                        values: values
                    }
                },
                y: {
                    label: {
                        text: 'Virtual cores',
                        position: 'outer-center'
                    }
                }
            },
            zoom: {enabled: true},
            point: {show: false},
            transition: {duration: 0}
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
           var obj = JSON.parse(e.target.result);
           this._jobs = obj['jobs']
           this._maxTime = obj['maxTime']
           this._maxResources = obj['maxResources']
           this._displayJobs()
        },this);
        reader.readAsText(e.target.files[0]);
    },

    save: function () {
        var obj = {
            jobs: this._jobs,
            maxTime: this._maxTime,
            maxResources: this._maxResources
        }
        var blob = new Blob([JSON.stringify(obj, undefined, 4)], {type: 'text/plain'}),
            e = document.createEvent('MouseEvents'),
            a = document.createElement('a');

        a.download = Date.now() + '.json';
        a.href = window.URL.createObjectURL(blob);
        a.dataset.downloadurl =  ['text/csv', a.download, a.href].join(':');
        e.initMouseEvent('click', true, false, window, 0, 0, 0, 0, 0, false, false, false, false, 0, null);
        a.dispatchEvent(e);
    }
});
