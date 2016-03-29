H.JobCompletionBar = H.Class.extend({

    initialize: function () {
        this._jobs = {};
        this._chart = c3.generate({
            bindto: '#jobcompl-chart',
            data: {
                x: 'x',
                type: 'bar',
                order: null,
                columns: []
            },
            bar: {
                width: {
                    ratio: 0.5 // this makes bar width 50% of length between ticks
                }
            },
            grid: {
                y: {
                    show: true
                }
            },
            axis: {
                x: {
                    type: 'category'
                },
                y: {
                    label: 'Time (s)'
                }
            }
        });
        $('#jobcompl-table').append('<thead><tr><th></th><th>ID</th><th>Name</th><th>Start time</th></tr></thead><tbody>');
        this._queryJobs();
    },

    _queryJobs: function () {
        $.ajax({
            dataType: "json",
            url: H.historyServerHost + '/mapreduce/jobs/',
            data: {},
            success: H.bind(this._handleJobQuery, this)
        });
    },

    _handleJobQuery: function (data) {
        if (!data.jobs || !data.jobs.job) {
            alert('No jobs found');
            return;
        }
        var jobs = data.jobs.job;
        for (var i = jobs.length - 1; i >= 0; i--) {
            this._jobs[jobs[i].id] = jobs[i];
            $('#jobcompl-table > tbody:last-child').append(
                '<tr>' +
                    '<td><input type="checkbox" id=' + jobs[i].id + '></td>' +
                    '<td>' + jobs[i].id + '</td>' +
                    '<td>' + jobs[i].name + '</td>' +
                    '<td>' + new Date(jobs[i].submitTime) + '</td>' +
                '</tr>');
        }
        $('input:checkbox').on('change', H.bind(this._updateGraph, this));
    },

    _updateGraph: function () {
        var toPlot = $('input:checkbox:checked');
        var x = ['x'];
        var wait = ['Waiting'];
        var run = ['Running'];

        for (var i = 0; i < toPlot.length; i++) {
            var id = toPlot[i].id;
            x.push(id);
            wait.push((this._jobs[id].startTime - this._jobs[id].submitTime) / 1000);
            run.push((this._jobs[id].finishTime - this._jobs[id].startTime) / 1000);
        }

        this._chart.load({
            columns: [x, wait, run],
        });
        this._chart.groups([['Waiting', 'Running']]);
    }
});
