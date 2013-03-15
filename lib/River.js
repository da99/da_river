var _     = require('underscore')
, Emitter = require('events').EventEmitter
, Job     = require('da_river/lib/Job').Job
,  h      = eval(require('da_river/lib/helpers').init())
;

// ****************************************************************
// ****************** River ***************************************
// ****************************************************************


function River() {}
exports.River = River;
River.uniq_id = 0;

River.new = function () {

  var me = new River;
  me.job_list     = [];
  me.waits        = [];
  me.replys       = [];
  me.data         = {};
  me.uniq_job_id  = 0;
  me.is_river     = true;
  me.emitter      = new Emitter;
  me.parent_job   = find_job(arguments);
  read_able(me);

  return me;
};


River.prototype.has_error = function (type, err, job) {
  var me = this;

  if (!arguments.length)
    return !!me.error;

  me.is_fin = true;

  me.error = err;
  if (!err.job)
    err.job = job;
  if (!err.type)
    err.type = type;

  return me;
};

River.prototype.set_finish = function (f) {
  this.set('finish', f);
  return me;
};

River.prototype.before_each = function (func) {
  var me = this;
  me.emitter.on('before job', func);
  return me;
};

River.prototype.next = function (type, func) {
  if (!this.for_next_job)
    this.for_next_job = [];
  this.for_next_job.push([type,func]);
  return this;
};

River.prototype.next_empty = function (raw_f) {
  if (!this.rel_jobs)
    this.rel_jobs = []

  this.rel_jobs.push([ null, null, function (j, last_reply) {
    if (!last_reply ||
        (_.isString(last_reply) && last_reply.trim() === '') ||
          (_.isObject(last_reply)  && _.isEmpty(last_reply))
       )
      raw_f.apply(null, arguments);
    else
      j.finish(last_reply);
  }]);
  return this;
};

River.prototype.job = function () {

  var me     = this;
  var args   = _.toArray(arguments);
  var args_l = args.length;
  var job    = null;
  switch (args_l) {
    case 2:
      var func  = args.pop();
      var group = args.pop();
      var id    = args.pop();
      break;
    default:
      var func  = args.pop();
      var id    = args.pop();
      var group = args.pop();
  };

  if (group === undefined)
    group = 'no group';
  if (id === undefined)
    id = ++this.uniq_job_id;

  job = Job.new({
    group     : group,
    id        : id,
    func      : func,
    river     : me,
  });

  _.each((me.for_next_job || []), function (pair) {
    job.set.apply(job, pair);
  });

  me.for_next_job = null;

  me.job_list.push(job);

  var rel_jobs = me.rel_jobs || [];
  me.rel_jobs = null;

  _.each(rel_jobs, function (triple) {
    var args = triple.slice();
    if (!args[0])
      args[0] = job.group;
    if (!args[1])
      args[1] = job.id + '-empty';
    me.job.apply(me, triple);
  });

  return me;
};

River.prototype.reply_counter = -1;

River.prototype.reply_for = function (group, id) {
  var me = this;
  var reply = me.reply_s_for(group, id);
  return reply[0];
};

River.prototype.replys_for = function (group, id) {
  var me = this;
  var replys = [];
  var use_both = _.compact([group, id]).length === 2;
  _.find(me.replys, function (hash) {
    var name = hash.name;
    if (use_both) {
      if (name === group+':'+id) {
        replys.push(hash.val);
        return true;
      }
    } else {
      if (name.index_of(group+':') > -1)
        replys.push(hash.val);
    }
    return false;
  });

  return replys;
};

River.prototype.first_reply = function () {
  return ( _.first(this.replys) || {} ).val;
};

River.prototype.last_reply = function () {
  return ( _.last(this.replys) || {} ).val;
};

River.prototype.job_must_find = function () {
  var args = _.toArray(arguments);
  var f = args.pop();

  var f_new = function (j) {
    j.reply(function (j, last) {
      if (last && ((_.isArray(last)) ? last.length : true))
        j.finish(last);
      else {
        var err = new Error('At least one reply required. Value: ' + JSON.stringify(last));
        err.job = j;
        j.finish('not_found', err, j);
      }
    });

    f(j);
  };

  args.push(f_new);
  this.job.apply(this, args);
  return this;
};

River.prototype.job_finish = function (job) {
  var me = this;

  if (job.has_error()) {
    return me.has_error(job.error.type, job.error);
  }

  me.replys.push({group: job.group, id: job.id, val: job.result});

  me.emitter.emit('after job', job);

  if (!me.waits.length) {
    return me.finish();
  }

  if (me.waits.length)
    me.run_job();

  return null;
};

River.prototype.is_finished = function () {
  var me = this;
  return !!this.is_fin || (parent(me) && parent(me).is_finished());
};

River.prototype.finish = function (j, unk) {
  var me     = this;

  if (me.is_fin)
    return null;

  if (j && j.is_job && !j.has_error())
    return me.job_finish(j);

  me.is_fin = true;

  var args = arguments;

  var fin = {
    river: me,
    finish: function () {
      var args = arguments;
      if (args.length === 2) {
        me.has_error(args[0], args[1]);
      } else if (args.length !== 0) {
        me.has_error('error', new Error('Unknown arguments: ' + _.toArray(arguments)));
      }

      if (parent(me))
        return parent(me).finish(me);

      if (me.has_error()) {
        throw me.error;
      }

      return me;
    }
  };

  if (!args.length && !me.waits.length && !me.has_error()) {

    if (me.get('finish')) {
      var func = me.get('finish');
      me.set('finish', null);
      return func(fin);
    }

    if (parent(me))
      return parent(me).finish(me.last_reply());

    return null;
  }

  if ((j && j.is_job && j.has_error()) || args.length === 2) {
    if (j && j.is_job)
      me.has_error(j.error.type, j.error);
    else
      me.has_error(j, unk);

    if (me.get(me.error.type)) {
      var func = me.get(me.error.type);
      me.set(me.error.type, null);
      return func(fin);
    }
  }

  return fin.finish();
};

River.prototype.verbose = function () {
  this.before_each(function (j) {
    console['log'](j.group, j.id);
  });
  return this;
};

River.prototype.run_job = function () {
  if (this.has_error())
    return this;
  var me  = this;
  var job = me.job_list[me.waits.shift()];

  me.emitter.emit('before job', job);
  job.func(job, me.last_reply());

  // Very little is reached below this line...
  // because:
  //   run_job
  //    finish
  //      run_job
  //        finish
};

River.prototype.run = function (f) {
  if (this.is_running)
    return this.finish('error', new Error('Already running.'));
  this.is_running = true;

  if (f)
    this.set('finish', f);

  var me     = this;
  this.waits = _.map(this.job_list, function (j, i) {
    return i;
  });

  if ( !this.waits.length ) {
    return this;
  }

  me.run_job();
  return me;
};










