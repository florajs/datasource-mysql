'use strict';

module.exports = function (grunt) {
    grunt.initConfig({
        mochaTest: {
            unit: {
                options: {
                    reporter: 'spec',
                    quiet: false
                },
                src: ['test/unit/*.spec.js']
            },
            integration: {
                options: {
                    reporter: 'spec',
                    quiet: false
                },
                src: ['test/integration/*.spec.js']
            },
            bamboo: {
                options: {
                    reporter: 'mocha-bamboo-reporter',
                    quiet: false
                },
                src: ['<%= mochaTest.unit.src %>']
            }
        },

        'mocha_istanbul': {
            coverage: {
                src: 'test',
                options: {
                    coverageFolder: 'build',
                    reportFormats: ['clover', 'lcov']
                }
            }
        },

        eslint: {
            target: ['lib/**/*.js', 'index.js']
        },

        clean: {
            build: {
                src: ['build/']
            }
        }
    });

    require('load-grunt-tasks')(grunt);

    grunt.registerTask('default', ['lint', 'test']);
    grunt.registerTask('lint', 'eslint');
    grunt.registerTask('test-unit', 'mochaTest:unit');
    grunt.registerTask('test-integration', 'mochaTest:integration');
    grunt.registerTask('test-bamboo', 'mochaTest:bamboo');
    grunt.registerTask('test-cov', ['mocha_istanbul:coverage']);
    grunt.registerTask('test', 'test-unit');
};
