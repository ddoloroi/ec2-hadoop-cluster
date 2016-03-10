H.UIBinding = H.Class.extend({

    initialize: function () {
        $('#jobdist').on('click', function () {
            H.jobDistribution = new H.JobDistribution();
        });
    },
});

