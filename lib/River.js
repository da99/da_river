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
  me.running_job  = null;
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
  return me.replys_for(group, id)[0];
};

River.prototype.reply_for = function (group, id) {
  var target = _.find(this.replys, function (v) {
    return v.group === group && v.id === id;
  });
  return (target || {}).val;
};

River.prototype.replys_for = function (group, id) {
  var me     = this;
  var use_both = arguments.length === 2;
  return _.pluck(_.filter(me.replys, function (r) {
    if (use_both)
      return r.group === group && r.id === id;
    else
      return r.group === group;
    return false;
  }), 'val');
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

  var f_new = function (j, last) {
    j.reply(function (j, last) {
      if (last && ((_.isArray(last)) ? last.length : true))
        j.finish(last);
      else {
        if (j.group && j.group.toUpperCase() !== 'NO GROUP' && j.id)
          var err = new Error(j.group + ', ' + j.id + ', not found.');
        else
          var err = new Error('At least one reply required. Value: ' + JSON.stringify(last));
        err.job = j;

        j.finish('not_found', err, j);
      }
    });

    f(j, last);
  };

  args.push(f_new);
  this.job.apply(this, args);
  return this;
};


River.prototype.is_finished = function () {
  var me = this;
  return !!this.is_fin || (parent(me) && parent(me).is_finished());
};

River.prototype.finish = function (rep, err) {
  var me   = this;
  var args = arguments;

  if (me.is_fin)
    return null;

  var job = (me.running_job || {}).job;
  me.running_job = null;

  if (job) {
    if (args.length < 2) {
      me.replys.push({group: job.group, id: job.id, val: job.result});
      if (me.waits.length)
        return me.run_job();
    }
  }

  if (args.length > 1 && !me.is_fin) {
    if (!err || !err.message)
      err = new Error('' + err);
    if (!err.type)
      err.type = rep;
    me.error = err;
  }

  me.is_fin = true;

  var fin = {
    river: me,
    finish: function () {

      if (parent(me)) {
        if (me.has_error())
          return parent(me).finish(me.error.type, me.error);
        else
          return parent(me).finish(me.last_reply());
      }

      if (me.has_error())
        throw me.error;

      return me;
    }
  };

  if (me.error) {
    var err_func = me.erase(me.error.type);
    if (err_func)
      return err_func(fin);
    else
      return fin.finish();
  }

  var fin_func = me.erase('finish');
  if (fin_func)
    return fin_func(fin, me.last_reply());
  else
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
  me.running_job = me.waits.shift();
  var job = me.job_list[me.running_job.i];

  me.emitter.emit('before job', job);
  job.func(job, me.last_reply());
};

River.prototype.run = function (f) {
  if (this.is_running)
    return this.finish('error', new Error('Already running.'));
  this.is_running = true;

  if (f)
    this.set('finish', f);

  var me     = this;
  this.waits = _.map(this.job_list, function (j, i) {
    return {group: j.group, id: j.id, job: j, i: i};
  });

  if ( !this.waits.length ) {
    return this;
  }

  me.run_job();
  return me;
};










