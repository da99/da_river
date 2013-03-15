
var _     = require('underscore')
, assert  = require('assert')
, River   = require('da_river/lib/River').River
, h       = require('da_river/test/helpers/main')
;

var vals = h.vals;

describe( 'Job', function () {

  describe( '.finish not_valid', function () {
    it( 'stops river', function () {
      var r = River.new(null);
      var job = null;
      r
      .job('runs', 1, function (j) { j.finish(j.id); })
      .job('runs', 2, function (j) { j.finish(j.id) })

      .next('invalid', function (j) {
        assert(j.job.about_error.msg, 3);
      })

      .job('runs', 3, function (j) {
        job = j;
        j.finish('not_valid', j.id);
      })

      .job('runs', 4, function (j) { j.finish(j.id) })

      .run(function () {
        throw new Error('Should not get here.');
      });

      assert.deepEqual(vals(r.replys), [1,2]);
      assert.equal(job.about_error.msg.message, 3);
    });
  }); // === describe


  describe( '.finish not_found', function () {
    it( 'stops river', function () {
      var results = [];
      var r = River.new(null);
      r
      .job('emit not found', 1, function (j) {
        j.set('not_found', function (flow) {
          var j = flow.job;
          results.push([j.id, j.about_error.msg.message]);
        })
        j.finish('not_found', "done");
      })
      .job('emit error', 2, function (j) {
        throw new Error('This is not supposed to be run after .not_found().');
      })
      .run(function (r) {
        throw new Error('This is not supposed to be run after .not_found().');
      });
      assert.deepEqual(results, [[1, 'done']]);
    });
  }); // === describe


  describe( '.finish', function () {
    it( 'turns string msg into an Error', function () {
      var results = null;
      River.new(null)
      .set('error', function (flow) {
        results = flow.river.about_error.msg;
      })
      .job(function (j) { j.finish(1); })
      .job(function (j) { j.finish('error', "This is error."); })
      .job(function (j) { j.finish(3); })
      .run();
      assert.equal(results.message, 'This is error.');
    });
  }); // === end desc

  describe( 'job .reply', function () {

    it( 'runs function before finish function', function () {
      var r = [];
      River.new(null)
      .job(function (j) {
        j.reply(function (j, result) {
          r.push(result);
          return j.finish(2);
        });

        j.finish(1);
      }).run();

      assert.deepEqual(r, [1]);
    });

    it( 'saves each value to replys Array', function () {
      var r = [];

      River.new(null)

      .job(function (j) {

        j.reply(function (j, result) {
          r.push(result);
          return j.finish(2);
        });

        j.finish(1);
      })

      .job(function (j, last) { r.push(last); })
      .run();

      assert.deepEqual(r, [1, 2]);
    });
  }); // === end desc


}); // === describe