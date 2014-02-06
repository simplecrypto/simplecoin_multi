module.exports = (grunt) ->

  # Project configuration.
  grunt.initConfig

    # Metadata.
    pkg: grunt.file.readJSON("package.json")
    banner: "/*! <%= pkg.title || pkg.name %> - v<%= pkg.version %> - " + "<%= grunt.template.today(\"yyyy-mm-dd\") %>\n" + "<%= pkg.homepage ? \"* \" + pkg.homepage + \"\\n\" : \"\" %>" + "* Copyright (c) <%= grunt.template.today(\"yyyy\") %> <%= pkg.author.name %>;" + " Licensed <%= _.pluck(pkg.licenses, \"type\").join(\", \") %> */\n"

    shell:
      options:
        stdout: true
        stderr: true
      reload:
        command: 'uwsgi --stop uwsgi.pid; sleep 1.5; uwsgi --yaml uwsgi.yml; sleep 1; tail uwsgi.log'

    watch:
      dev_server:
        files: ['simpledoge/**/*.py', 'config.yml', 'uwsgi.yml']
        tasks: ['shell:reload']
        options:
          atBegin: true

  grunt.loadNpmTasks('grunt-shell')
  grunt.loadNpmTasks('grunt-contrib-watch')
