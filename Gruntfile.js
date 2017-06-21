'use strict';

module.exports = (grunt) => {
    grunt.initConfig({
        shell: {
            cleanup: {
                command: 'docker-compose -f test/docker-compose.yml rm --force'
            },
            mysql: {
                command: 'docker-compose -f test/docker-compose.yml up -d mysql'
            },
            test: {
                command: 'docker-compose -f test/docker-compose.yml up --abort-on-container-exit test'
            },
            bamboo: {
                command: 'docker-compose -f test/docker-compose.yml up --abort-on-container-exit test',
                options: {
                    execOptions: {
                        env: Object.assign({}, process.env, {
                            GRUNT_TARGET: 'mochaTest:bamboo',
                            COMPOSE_OPTIONS: '-e GRUNT_TARGET=mochaTest:bamboo' // containerized docker-compose
                        })
                    }
                }
            },
            coverage: {
                command: 'docker-compose -f test/docker-compose.yml up --abort-on-container-exit test',
                options: {
                    execOptions: {
                        env: Object.assign({}, process.env, {
                            GRUNT_TARGET: 'mocha_istanbul:coverage',
                            COMPOSE_OPTIONS: '-e GRUNT_TARGET=mocha_istanbul:coverage' // containerized docker-compose
                        })
                    }
                }
            },
            kill: {
                command: [
                    'docker-compose -f test/docker-compose.yml stop',
                    'docker-compose -f test/docker-compose.yml kill'
                ].join(' && ')
            }
        },

        mochaTest: {
            stdout: {
                options: {
                    reporter: 'spec',
                    quiet: false
                },
                src: ['test/*.spec.js']
            },
            bamboo: {
                options: {
                    reporter: 'mocha-bamboo-reporter',
                    quiet: false
                },
                src: ['<%= mochaTest.stdout.src %>']
            }
        },

        mocha_istanbul: {
            coverage: {
                src: 'test',
                options: {
                    mask: '*.spec.js',
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
    grunt.registerTask('test', ['shell:cleanup', 'shell:mysql', 'shell:test', 'shell:kill']);
    grunt.registerTask('test-bamboo', ['shell:cleanup', 'shell:mysql', 'shell:bamboo', 'shell:kill']);
    grunt.registerTask('test-cov', ['shell:cleanup', 'shell:mysql', 'shell:coverage', 'shell:kill']);
};
