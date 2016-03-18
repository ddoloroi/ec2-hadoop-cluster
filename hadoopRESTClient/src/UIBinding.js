H.UIBinding = H.Class.extend({

    initialize: function () {
        $('#jobdist').on('click', function () {
            H.jobDistribution = new H.JobDistribution();
        });

        $('#jobdist-download').on('click', function () {
            svgDownload('jobdist-chart');
        });
    },

});

function svgDownload(id) {
    $.ajax({
        url: "c3/c3.min.css",
        dataType: "text",
        success: function (css) {
            css = css.replace(/\.c3 /g, '');
            var target = $('#' + id + ' svg')[0];
            target = document.getElementsByTagName('svg')[0];
            //getStyles(target);

            var s = document.createElement('style');
            s.setAttribute('type', 'text/css');
            s.innerHTML = "<![CDATA[\n" + css + "\n]]>";

            var defs = document.createElement('defs');
            defs.appendChild(s);
            target.insertBefore(defs, target.firstChild);

            var wrap = document.createElement('div');
            wrap.appendChild(target.cloneNode(true));
            var html = wrap.innerHTML;

            var dataURL = 'data:image/svg+xml;base64,'+ btoa(html);
            var a = document.createElement('a');
            a.href = dataURL;
            a.download = id + '.svg';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
        }
    });
}

