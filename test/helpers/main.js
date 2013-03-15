var _ = require('underscore')
;

exports.vals = function (river) {
  if (_.isArray(river))
    var arr = river;
  else
    var arr = river.replys;

  return _.map(arr, function (v) {
    return v.val;
  });
}

