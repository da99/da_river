
var _     = require('underscore')
, assert  = require('assert')
, River   = require('da_river/lib/River').River
, h       = require('da_river/test/helpers/main')
;

var vals = h.vals;

describe( 'River', function () {

  it( 'runs job after the previous one finishes', function (done) {
    var r = River.new(null);
    var results = [];

    r
    .job('del: ', 'job-keys', function (j) {
      process.nextTick(function (){
        j.finish(1);
      });
    })

    .job('pop: ', 0, function (j) {
      j.finish(2);
    })

    .job('insert: ', 1, function (j) {
      process.nextTick( function() {
        j.finish(3);
      });
    })

    .job('pop: ', 1, function (j) {
      j.finish(4);
    })

    .run(function () {
      assert.deepEqual([1, 2, 3, 4], vals(r.replys));
      done();
    })
    ;

  });

  it( 'throws Error if no error handler defined', function () {
    var results = [];
    var r = River.new(null);
    r
    .job('throw', 'done', function (j) {

      var e = null;
      try {
        j.finish('something', 'done');
      } catch(err) {
        e = err;
      }
      assert.equal(e.type + ': ' + e.message, 'something: done');
    })
    .run();
  });

  describe( '.before_each', function () {

    it( 'runs callback before each job', function () {
      var replys = [];
      River.new(null)
      .job('a', 1, function (j) { j.finish("a"); })
      .job('a', 9, function (j) { j.finish("a"); })
      .before_each(function (j) { replys.push(j.id); })
      .run();
      assert.deepEqual(replys, [1,9]);
    });

    it( 'passes job to callback', function () {
      var replys = [];
      River.new(null)
      .job('a', 1, function (j) { j.finish("a"); })
      .job('a', 9, function (j) { j.finish("a"); })
      .before_each(function (j) { replys.push(j.is_job); })
      .run();
      assert.deepEqual(replys, [true, true]);
    });
  }); // === end desc

  describe( '.finish error', function () {

    it( 'stops river', function (done) {
      var results = [];
      var r = River.new(null);
      r
      .set('error', function (flow) {
        var river = flow.river;
        results.push([river.about_error.type, river.about_error.msg.message]);
      })

      .job('emit error', 1, function (j) { j.finish('error', "done"); })
      .job('emit error', 2, function (j) { j.finish('error', "done " + j.id); })
      .run(function (r) {
        throw new Error('This is not supposed to be run after .error().');
      });
      assert.deepEqual(results, [['error', 'done']]);
      done();
    });
  }); // === describe

  describe( '.finish not_valid', function () {

    it( 'runs if error is of type .not_valid', function () {
      var results = [];
      var r = River.new(null);
      r
      .set('not_valid', function (flow) {
        var river = flow.river;
        results.push([river.about_error.type, river.about_error.msg.message]);
      })

      .job('emit error', 1, function (j) { j.finish('not_valid', "done"); })
      .run(function (r) {
        throw new Error('This is not supposed to be run after .error().');
      });
      assert.deepEqual(results, [['not_valid', 'done']]);
    });

    it( 'it does not run "error" handler', function () {
      var results = "...";
      River.new(null)
      .set('error'      , function (j) { results += "reached error"; })
      .set('not_valid'  , function (j) { results += "reached not valid"; })
      .job('emit error' , function (j) { j.finish('not_valid', "done"); })
      .run(function (r) {
        throw new Error('This is not supposed to be run after .error().');
      });

      assert.deepEqual(results, '...reached not valid');
    });
  }); // === describe

  describe( '.next', function () {

    it( 'runs event only in job', function (d) {
      var results = [];
      var r = River.new(null);
      r
      .next('invalid', function (j) {
        assert.equal(j.job.about_error.msg.message, 'don');
        assert.equal(j.job.is_job, true);
        d();
      })
      .job('push', 'don', function (j) { j.finish('invalid', 'don'); })
      .run();
    });

  }); // === describe

  describe( 'group/id', function () {
    it( 'group/id are optional', function (done) {
      var val = null;
      River.new(null)
      .job(function (j) {
        val = j.group + ' ' + j.id;
        j.finish(val);
      })
      .set('finish', function (r) {
        assert.equal(val, 'no group 1');
        done();
      })
      .run()
      ;
    });
  }); // === describe

  describe( 'inheriting a job', function () {

    it( 'runs the events of the previous job.river', function () {
      River.new(null)
      .set('error', function (flow) {
        var r = flow.river;
        assert.equal(r.about_error.msg.message, 'reached');
      })
      .job(function (j) {

        River.new(j)
        .job(function (j) { j.finish('error', 'reached'); })
        .run();

      })
      .run();
    });

    it( 'stops finishing if any ancesotors have stopped in error', function () {
      var val = 0;

      var b = River.new(null)
      .set('error', function (err) {
        ++val;
      })
      .job(function (j) {
        ++val;
        j.finish(val);
      })
      .job(function (top_j) {

        var a = River.new(top_j)
        .job(function (j) {
          ++val;
          top_j.finish('error', 'reached');
          top_j.finish(val);
          j.finish(val);
        })
        .job(function (j) { ++val; })
        .run();

        assert.equal(a.replys.length, 0);

      })
      .run();

      assert.deepEqual(vals(b.replys), [1]);
    });


  }); // === describe

  describe( '.next_empty', function () {

    _.each([null, undefined, false, 0], function (v) {
      it( 'runs function on a "' + v + '" response.', function () {
        var rep = "none";
        River.new(null).next_empty(function (j, last_reply) {
          rep = 'empty';
          j.finish(rep)
        })
        .job(function (j) { j.finish(v) })
        .run();
        assert.equal(rep, 'empty');
      });

      it( 'passes last reply to user-defined callback', function () {
        var rep = "none";
        River.new(null).next_empty(function (j, last_reply) {
          rep = last_reply;
          j.finish(rep)
        })
        .job(function (j) { j.finish(v) })
        .run();
        assert.equal(rep, v);
      });

    });

    it( 'does not run if reply is less than 0', function () {
      var rep = "none";
      River.new(null).next_empty(function (j, last_reply) {
        rep = 'ran';
        j.finish(rep)
      })
      .job(function (j) { j.finish(-1) })
      .run();
      assert.equal(rep, 'none');
    });

    it( 'does not run on object', function () {
      var rep = "none";
      River.new(null).next_empty(function (j, last_reply) {
        rep = 'ran';
        j.finish(rep)
      })
      .job(function (j) { j.finish({a: 'b'}) })
      .run();
      assert.equal(rep, 'none');
    });

    it( 'does not runs on empty function', function () {
      var rep = "none";
      River.new(null).next_empty(function (j, last_reply) {
        rep = 'ran';
        j.finish(rep)
      })
      .job(function (j) { j.finish(function () {}) })
      .run();
      assert.equal(rep, 'ran');
    });

    it( 'runs on whitespace string', function () {
      var rep = "none";
      River.new(null).next_empty(function (j, last_reply) {
        rep = 'ran';
        j.finish(rep)
      })
      .job(function (j) { j.finish("  ") })
      .run();
      assert.equal(rep, 'ran');
    });

    it( 'runs on empty Array', function () {
      var rep = "none";
      River.new(null).next_empty(function (j, last_reply) {
        rep = 'ran';
        j.finish(rep)
      })
      .job(function (j) { j.finish([]) })
      .run();
      assert.equal(rep, 'ran');
    });

    it( 'runs on empty Object: {}', function () {
      var rep = "none";
      River.new(null).next_empty(function (j, last_reply) {
        rep = 'ran';
        j.finish(rep)
      })
      .job(function (j) { j.finish({}) })
      .run();
      assert.equal(rep, 'ran');
    });

  }); // === end desc

  describe( '.job_not_empty', function () {

    it( 'finishes with error not_found if reply is: !reply == true', function (done) {
     River.new(null)
     .next('not_found', function (j) {
        assert.equal(j.job.about_error.msg.message, "At least one reply required. Value: false");
        done();
     })
     .job_not_empty(function (j) {
       process.nextTick(function () {
         j.finish(false);
       });
     }).run();
    });

    it( 'finishes with error not_found if reply is: [].length === 0', function (done) {
     River.new(null)
     .next('not_found', function (j) {
        assert.equal(j.job.about_error.msg.message, "At least one reply required. Value: []");
        done();
     })
     .job_not_empty(function (j) {
       process.nextTick(function () {
         j.finish([]);
       });
     }).run();
    });

    it( 'finishs with reply if reply: !!reply === true', function () {
     var r = River.new(null)
     .job_not_empty(function (j) {
       j.finish("hoppe");
     }).run();
     assert.equal(r.last_reply(), 'hoppe');
    });

  }); // === end desc

  describe( '.first_reply', function () {
    it( 'returns .val of first reply of .replys', function () {
      var r = River.new(null)
      .job(function (j) { j.finish(1); })
      .job(function (j) { j.finish(2); })
      .job(function (j) { j.finish(3); })
      .run();
      assert.equal(r.first_reply(), 1);
    });
  }); // === end desc

  describe( '.last_reply', function () {
    it( 'returns .val of last reply of .replys', function () {
      var r = River.new(null)
      .job(function (j) { j.finish(1); })
      .job(function (j) { j.finish(2); })
      .job(function (j) { j.finish(3); })
      .run();
      assert.equal(r.last_reply(), 3);
    });
  }); // === end desc

}); // === describe
