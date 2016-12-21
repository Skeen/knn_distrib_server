var express = require('express');
var multer  = require('multer');
var cors    = require('cors');
var link    = require('fs-symlink');
var mkdirp  = require('mkdirp');
var ls      = require('list-directory-contents');
var bodyParser = require('body-parser')
var shortid = require('shortid');
var EventEmitter = require('events');
var fs = require('fs');
var fileExists = require('file-exists');

var app = express();

app.use(cors());
app.use(bodyParser.json({ // to support JSON-encoded bodies
    limit: Number.POSITIVE_INFINITY
}));
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
})); 

var upload_folder  = 'uploads';
var split_folder   = 'splits';
var output_folder  = 'output';

// Ensure that folders are created
var folder_callback = function(err) { if(err) console.error(err); };
mkdirp(upload_folder, folder_callback);
mkdirp(split_folder, folder_callback);
mkdirp(output_folder, folder_callback);

// Serve contents of uploads folder
var directory = require('serve-index');
app.use('/uploads/', directory(upload_folder, {'icons': true}));
app.use('/uploads/', express.static(upload_folder + '/'));
// ... and of the splits folder
app.use('/splits/', directory(split_folder, {'icons': true}));
app.use('/splits/', express.static(split_folder + '/'));
// ... and of the output folder
app.use('/output/', directory(output_folder, {'icons': true}));
app.use('/output/', express.static(output_folder + '/'));

var task_queue = [];
var task_queue_emitter = new EventEmitter();

var exec = require('child_process').exec;
var num_lines_file = function(filepath, callback)
{
    exec('wc -l ' + filepath + ' | cut -f1 -d" "', callback); 
}

var split_file = function(file, num, callback)
{
    exec('split -l ' + (3 * num) + ' ../' + file.path + " " + file.filename, {cwd: split_folder}, callback); 
}

// Upload file to server
var upload = multer({ dest: upload_folder + '/' });
var cpUpload = upload.fields([{ name: 'query', maxCount: 1 }, { name: 'reference', maxCount: 1 }])
app.post('/knn', cpUpload, function(req, res, next) 
{
    // Query file
    var query = req.files.query[0];
    if(query === undefined)
    {
        res.status(400);
        res.end("Missing payload: query");
        return;
    }
    // TODO: Check validity of file

    // Reference file
    var reference = req.files.reference[0];
    if(reference === undefined)
    {
        res.status(400);
        res.end("Missing payload: reference");
        return;
    }
    // TODO: Check validity of file

    // Query split
    var query_split = req.body.split;
    // Calculation timeout
    var timeout = req.body.timeout;
    // TODO: Check that these are set

    num_lines_file(query.path, function(err, lines)
    {
        if(err)
        {
            res.status(500);
            res.end(JSON.stringify(err));
            return;
        }
        // Check lines are a multiple of 3
        if((lines % 3) != 0)
        {
            res.status(400);
            res.end("#Lines in query isn't a multiple of 3!");
            return;
        }
        split_file(query, query_split, function(err, lines)
        {
            if(err)
            {
                res.status(500);
                res.end(JSON.stringify(err));
                return;
            }
            ls('./' + split_folder, function(err, tree)
            {
                if(err)
                {
                    res.status(500);
                    res.end(JSON.stringify(err));
                    return;
                }
                var filtered = tree.filter(function(element)
                {
                    return element.startsWith(split_folder + '/' + query.filename);
                });
                var queries = filtered.map(function(element)
                {
                    return {
                        path: element, 
                        running: false,
                        done: false, 
                        timer: null, 
                        part: element.substring(element.indexOf('/') + 1).replace(query.filename, '')
                    };
                });

                var task = {
                    name: shortid.generate(),
                    query: queries,
                    query_full: query.path,
                    reference: reference.path,
                    timeout: timeout,
                    complete: function(res, task)
                    {
                        // Pull out the results
                        var results = task.query.map(function(element)
                        {
                            return element.result;
                        });
                        // Merge all results into a single blob
                        var merged = [].concat.apply([], results);
                        // Get a stringied version
                        var return_str = JSON.stringify(merged);
                        // Write this to the output folder
                        fs.writeFile(output_folder + '/' + task.name, return_str, function(err) 
                        {
                            if(err)
                            {
                                res.status(500);
                                res.end(JSON.stringify(err));
                                return;
                            }
                            task_queue_emitter.emit('complete', task.name, merged, return_str);
                        });
                    }.bind(null, res)
                }
                // Add the task
                task_queue.push(task);
                task_queue_emitter.emit('add');

                // Reply with the result
                res.status(200);
                res.end(task.name);
            });
        });
    });
});

var acquire_task = function(id, callback)
{
    if(id == task_queue.length)
    {
        callback("No undelegated tasks in queue!");
        return;
    }

    var task = task_queue[id];
    var queryIndex = task.query.findIndex(function(element)
    {
        return !element.running && !element.done;
    });
    if(queryIndex == -1)
    {
        acquire_task(id+1, callback);
        return;
    }

    task.query[queryIndex].running = true;
    task.query[queryIndex].timer = setTimeout(function(task, index)
    {
        console.info("Triggered reset timer", task.name, "index:", queryIndex);
        task.query[queryIndex].running = false;
        task.query[queryIndex].timer = null;
    }.bind(null,task,queryIndex), task.timeout);

    callback(undefined, task.query[queryIndex], task);
}

app.get('/requestRelease', function(req, res, next)
{
	taskqueue.forEach(
		function(task)
		{
			task.query.forEach(
				function(query)
				{
					query.timer = null;
				});
		});
}

app.get('/requestTask', function(req, res, next)
{
    if(task_queue.length == 0)
    {
        res.status(202);
        res.end("No tasks in queue!"); 
    }
    else
    {
        acquire_task(0, function(err, query, task)
        {
            if(err)
            {
                res.status(202);
                res.end(err);
                return;
            }
            res.status(200);

            res.end(JSON.stringify({
                reference: task.reference,
                query: query.path,
                part: query.part,
                name: task.name
            }));
        });
    }
});

app.post('/replyTask', function(req, res, next)
{
    var json = req.body;

    // Find the task we got something back from
    var queueIndex = task_queue.findIndex(function(element)
    {
        return element.name == json.name;
    });
    if(queueIndex == -1)
    {
        res.status(400);
        res.end("No task named: " + json.name);
        return;
    }
    // Find the query within that task
    var task = task_queue[queueIndex];
    var queryIndex = task.query.findIndex(function(element)
    {
        return element.path == json.query;
    });
    if(queryIndex == -1)
    {
        res.status(400);
        res.end("No sub-query named: " + json.query);
        return;
    }
    // Set that query as done
    clearTimeout(task.query[queryIndex].timer);
    task.query[queryIndex].done = true;
    task.query[queryIndex].running = false;
    task.query[queryIndex].timer = null;
    task.query[queryIndex].result = json.result;
    // Fire our update event
    // task_queue_emitter.emit('update', task, task.query[queryIndex])
    // TODO: Check if all are done
    var all_done = task.query.reduce(function(a, b)
    {
        return a && b.done;
    }, true);
    if(all_done)
    {
        console.log("Task:", task.name, "done!");
        task.complete(task);
        task_queue.splice(queueIndex, 1);
        task_queue_emitter.emit('remove', queueIndex);
    }

    res.status(200);
    res.end("Succes");
});

app.get('/tasksJSON', function(req, res, next)
{
    res.status(200);
    res.end(JSON.stringify(task_queue, function(key, value)
    {
        if(key == 'timer')
        {
            if(value == null)
            {
                return null;
            }
            else
            {
                return "Timer active!";
            }
        }
        else 
        {
            return value; 
        }
    }));
});

var peek_task = function(id, callback)
{
    if(id >= task_queue.length)
    {
        callback(false);
        return;
    }

    var task = task_queue[id];
    var queryIndex = task.query.findIndex(function(element)
    {
        return !element.running && !element.done;
    });
    if(queryIndex == -1)
    {
        peek_task(id+1, callback);
        return;
    }

    callback(true);
}

app.get('/awaitTask', function(req, res, next)
{
    peek_task(0, function(available, index)
    {
        if(available)
        {
            res.status(200);
            res.end("Task is directly available");
            return;
        }
        else
        {
            var callback = function()
            {
                res.status(200);
                res.end("Task available");
                task_queue_emitter.removeListener('add', callback);
            }

            task_queue_emitter.addListener('add', callback);
        }
    });
});

app.get('/awaitComplete', function(req, res, next)
{
    var name = req.query.name;

    // Find the task we got something back from
    var queueIndex = task_queue.findIndex(function(element)
    {
        return element.name == name;
    });
    if(queueIndex == -1)
    {
        var filename = output_folder + '/' + name;
        if(fileExists(filename))
        {
            fs.readFile(filename, 'utf8', function(err, data) 
            {
                if (err) 
                {
                    res.status(500);
                    res.end(JSON.stringify(err));
                    return;
                }
                res.status(200);
                res.end(data);
            });
        }
        else
        {
            res.status(400);
            res.end("No task named: " + name);
            return;
        }
    }
    else
    {
        var callback = function(task_name, json, str)
        {
            if(task_name == name)
            {
                res.status(200);
                res.end(str);
                task_queue_emitter.removeListener('complete', callback);
            }
        }

        // It's still running
        task_queue_emitter.addListener('complete', callback);
    }
});

app.get('/tasks', function(req, res, next)
{
    res.sendFile("tasks.html", {root: __dirname});
});

app.get('/', function(req, res)
{
    res.sendFile("index.html", {root: __dirname});
});

var port = 3001;
app.listen(port, function() 
{
    console.log('Reading reciever server on port: ' + port);
});
