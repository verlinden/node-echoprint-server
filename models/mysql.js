/**
 * MySQL database backend. An alternative database backend can be created
 * by implementing all of the methods exported by this module
 */

var fs = require('fs');
var mysql = require('mysql');
var temp = require('temp');
var config = require('../config');

exports.fpQuery = fpQuery;
exports.getTrack = getTrack;
exports.getTrackByName = getTrackByName;
exports.addTrack = addTrack;
exports.updateTrack = updateTrack;
exports.disconnect = disconnect;

// Initialize the MySQL connection
var client = mysql.createClient({
  user: config.db_user,
  password: config.db_pass,
  database: config.db_database,
  host: config.db_host
});

/**
 *
 */
function fpQuery(fp, rows, callback) {
  var fpCodesStr = fp.codes.join(',');
  
  // Get the top N matching tracks sorted by score (number of matched codes)
  var sql = 'SELECT track_id,COUNT(track_id) AS score ' +
    'FROM codes ' +
    'WHERE code IN (' + fpCodesStr + ') ' +
    'GROUP BY track_id ' +
    'ORDER BY score DESC ' +
    'LIMIT ' + rows;
  client.query(sql, [], function(err, matches) {
    if (err) return callback(err, null);
    if (!matches || !matches.length) return callback(null, []);
    
    var trackIDs = new Array(matches.length);
    var trackIDMap = {};
    for (var i = 0; i < matches.length; i++) {
      var trackID = matches[i].track_id;
      trackIDs[i] = trackID;
      trackIDMap[trackID] = i;
    }
    var trackIDsStr = trackIDs.join('","');
    
    // Get all of the matching codes and their offsets for the top N matching
    // tracks
    sql = 'SELECT code,time,track_id ' +
      'FROM codes ' +
      'WHERE code IN (' + fpCodesStr + ') ' +
      'AND track_id IN ("' + trackIDsStr + '")';
    client.query(sql, [], function(err, codeMatches) {
      if (err) return callback(err, null);
      
      for (var i = 0; i < codeMatches.length; i++) {
        var codeMatch = codeMatches[i];
        var idx = trackIDMap[codeMatch.track_id];
        if (idx === undefined) continue;
        
        var match = matches[idx];
        if (!match.codes) {
          match.codes = [];
          match.times = [];
        }
        match.codes.push(codeMatch.code);
        match.times.push(codeMatch.time);
      }
      
      callback(null, matches);
    });
  });
}

function getTrack(trackID, callback) {
  var sql = 'SELECT tracks.* ' +
    'FROM tracks ' +
    'WHERE tracks.id=? ';
  client.query(sql, [trackID], function(err, tracks) {
    if (err) return callback(err, null);
    if (tracks.length === 1)
      return callback(null, tracks[0]);
    else
      return callback(null, null);
  });
}

function getTrackByName(track, callback) {
  var sql = 'SELECT tracks.* ' +
    'FROM tracks ' +
    'WHERE tracks.name LIKE ? ';
  client.query(sql, [track], function(err, tracks) {
    if (err) return callback(err, null);
    if (tracks.length > 0)
      return callback(null, tracks[0]);
    else
      return callback(null, null);
  });
}

function addTrack(fp, callback) {
  var length = fp.length;
  if (typeof length === 'string')
    length = parseInt(length, 10);
  
  var sql = 'INSERT INTO tracks ' +
    '(name,length,import_date) ' +
    'VALUES (?,?,?)';
  client.query(sql, [fp.track, length, new Date()],
    function(err, info)
  {
    if (err) return callback(err, null);
    if (info.affectedRows !== 1) return callback('Track insert failed', null);
    
    var trackID = info.insertId;
    
    // Write out the codes to a file for bulk insertion into MySQL
    var tempName = temp.path({ prefix: 'echoprint-' + trackID, suffix: '.csv' });
    writeCodesToFile(tempName, fp, trackID, function(err) {
      if (err) return callback(err, null);
      
      // Bulk insert the codes
      sql = 'LOAD DATA INFILE ? IGNORE INTO TABLE codes';
      client.query(sql, [tempName], function(err, info) {
        // Remove the temporary file
        fs.unlink(tempName, function(err2) {
          if (!err) err = err2;
          callback(err, trackID);
        });
      });
    });
  });
}

function writeCodesToFile(filename, fp, trackID, callback) {
  var i = 0;
  var keepWriting = function() {
    var success = true;
    while (success && i < fp.codes.length) {
      success = file.write(fp.codes[i]+'\t'+fp.times[i]+'\t'+trackID+'\n');
      i++;
    }
    if (i === fp.codes.length)
      file.end();
  };
  
  var file = fs.createWriteStream(filename);
  file.on('drain', keepWriting);
  file.on('error', callback);
  file.on('close', callback);
  
  keepWriting();
}

function updateTrack(trackID, name, callback) {
  var sql = 'UPDATE tracks SET name=? WHERE id=?';
  client.query(sql, [name, trackID], function(err, info) {
    if (err) return callback(err, null);
    callback(null, info.affectedRows === 1 ? true : false);
  });
}

function disconnect(callback) {
  client.end(callback);
}