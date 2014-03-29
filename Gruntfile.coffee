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
        command: 'kill -2 `cat gunicorn.pid`; gunicorn simplecoin.wsgi_entry:app -D -p gunicorn.pid -b 0.0.0.0:9400 --access-logfile gunicorn.log; tail gunicorn.log; tail webserver.log'

    watch:
      dev_server:
        files: ['simplecoin/**/*.py', 'config.yml', 'uwsgi.yml']
        tasks: ['shell:reload']
        options:
          atBegin: true
      static:
        files: ['static/css/*.css', 'templates/*.html']
        options:
          livereload: true

  grunt.loadNpmTasks('grunt-shell')
  grunt.loadNpmTasks('grunt-contrib-watch')
