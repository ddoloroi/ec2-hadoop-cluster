module.exports = function(grunt) {
    grunt.initConfig({
        concat: {
            options: {
                separator: ';',
            },
            client: {
                src: [
                    'src/Hadoop.js',
                    'src/Class.js',
                    'src/Util.js',
                    'src/Main.js'
                ],
                dest: 'dist/hadoop.js'
            }
        },
    });

    grunt.loadNpmTasks('grunt-contrib-concat');

    grunt.registerTask('default', ['concat']);
    grunt.registerTask('build', ['concat']);
};
