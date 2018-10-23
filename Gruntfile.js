'use strict';

module.exports = (grunt) => {
    const uid = process.getuid();

    grunt.initConfig({
        shell: {
            cleanup: {
                command: 'docker-compose -f test/integration/docker-compose.yml rm --force'
            },
            mysql: {
                command: 'docker-compose -f test/integration/docker-compose.yml up -d mysql'
            },
            test: {
                command: 'docker-compose -f test/integration/docker-compose.yml up --abort-on-container-exit test'
            },
            bamboo: {
                command: 'docker-compose -f test/integration/docker-compose.yml up --abort-on-container-exit test',
                options: {
                    execOptions: {
                        env: Object.assign({}, process.env, {
                            UID: uid,
                            GRUNT_TARGET: 'mochaTest:integration',
                            // containerized docker-compose
                            COMPOSE_OPTIONS: [
                                '-e GRUNT_TARGET=mochaTest:integration',
                                `-e UID=${uid}`
                            ].join(' ')
                        })
                    }
                }
            },
            coverage: {
                command: 'docker-compose -f test/integration/docker-compose.yml up --abort-on-container-exit test',
                options: {
                    execOptions: {
                        env: Object.assign({}, process.env, {
                            UID: uid,
                            GRUNT_TARGET: 'mocha_istanbul:coverage',
                            // containerized docker-compose
                            COMPOSE_OPTIONS: [
                                '-e GRUNT_TARGET=mocha_istanbul:coverage',
                                `-e UID=${uid}`
                            ].join(' ')
                        })
                    }
                }
            },
            kill: {
                command: [
                    'docker-compose -f test/integration/docker-compose.yml stop',
                    'docker-compose -f test/integration/docker-compose.yml kill'
                ].join(' && ')
            }
        },

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
    grunt.registerTask('test-unit', 'mochaTest:unit');
    grunt.registerTask('test-integration', ['shell:cleanup', 'shell:mysql', 'shell:test', 'shell:kill', 'shell:cleanup']);
    grunt.registerTask('test', ['test-unit', 'test-integration']);
    grunt.registerTask('test-bamboo', ['shell:cleanup', 'shell:mysql', 'shell:bamboo', 'shell:kill', 'shell:cleanup']);
    grunt.registerTask('test-cov', ['shell:cleanup', 'shell:mysql', 'shell:coverage', 'shell:kill', 'shell:cleanup']);
};
