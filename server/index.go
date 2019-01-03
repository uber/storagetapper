// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package server

import (
	"net/http"

	"github.com/uber/storagetapper/log"
)

var indexHTML = `<!doctype html>
<html lang="en">
	<head>
		<meta charset="utf-8">
		<meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
		<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/css/bootstrap.min.css" integrity="sha384-Gn5384xqQ1aoWXA+058RXPxPg6fy4IWvTNh0E263XmFcJlSAwiGgFAW/dAiS6JXm" crossorigin="anonymous">
		<title>StorageTapper</title>
	</head>
	<body>
		<nav class="navbar navbar-dark bg-dark">
			<a class="navbar-brand" href="#">StorageTapper</a>
		</nav>
		<div>
			<ul class="nav nav-tabs bg-dark" id="tabs">
				<li class="nav-item"><a class="nav-link active" href="#tables" role="tab" data-toggle="tab">Tables</a></li>
				<li class="nav-item"><a class="nav-link" href="#clusters" role="tab" data-toggle="tab">Clusters</a></li>
				<li class="nav-item"><a class="nav-link" href="#schemas" role="tab" data-toggle="tab">Schemas</a></li>
				<li class="nav-item"><a class="nav-link" href="#options" role="tab" data-toggle="tab">Settings</a></li>
			</ul>

			<div class="tab-content">

				<div role="tabpanel" class="tab-pane active" id="tables">
					<div class="row m-3">
						<div class="col">
							<button class="btn btn-success float-left" data-toggle="modal" data-target="#add_table">&#10133; Register</button>
						</div>
						<div class="col-auto">
							<form class="form-inline my-2 my-lg-0">
								<input id="table_filter" type="search" class="form-control mr-sm-2" placeholder="Search">
							</form>
						</div>
					</div>

					<table class="table table-hover m-2" id="table_list">
						<thead class="thead-light">
							<tr>
								<th scope="col"></th>
								<th scope="col">UpdatedAt</th>
								<th scope="col">Service</th>
								<th scope="col">Cluster</th>
								<th scope="col">Database</th>
								<th scope="col">Table</th>
								<th scope="col">Input</th>
								<th scope="col">Format</th>
								<th scope="col">Output</th>
								<th scope="col">Version</th>
								<th scope="col">SnapshottedAt</th>
								<th scope="col"></th>
							</tr>
						</thead>
						<tbody>
						</tbody>
					</table>
					<div class="row">
						<div class="col">
							<ul class="pagination justify-content-center">
								<li class="page-item" id="table_list_prev"><a class="page-link" href="#">Previous</a></li>
								<li class="page-item" id="table_list_next"><a class="page-link" href="#">Next</a></li>
							</ul>
						</div>
						<div class="col-auto mr-2">
							<ul class="pagination justify-content-start">
								<li class="page-item active" data-pager="table" data-pagesize="25"><a class="page-link" href="#">25</a></li>
								<li class="page-item" data-pager="table" data-pagesize="50"><a class="page-link" href="#">50</a></li>
								<li class="page-item" data-pager="table" data-pagesize="100"><a class="page-link" href="#">100</a></li>
							</ul>
						</div>
					</div>
				</div>

				<div role="tabpanel" class="tab-pane" id="clusters">
					<div class="row m-3">
						<div class="col">
							<button class="btn btn-success float-left" data-toggle="modal" data-target="#add_cluster">&#10133; New</button>
						</div>
						<div class="col-auto">
							<form class="form-inline my-2 my-lg-0">
								<input id="cluster_filter" type="search" class="form-control mr-sm-2" placeholder="Search">
							</form>
						</div>
					</div>
					<table class="table m-2" id="cluster_list">
						<thead>
							<tr>
								<th scope="col"></th>
								<th scope="col">Name</th>
								<th scope="col">Host</th>
								<th scope="col">Port</th>
								<th scope="col">User</th>
								<th scope="col"></th>
							</tr>
						</thead>
						<tbody>
						</tbody>
					</table>
					<div class="row">
						<div class="col">
							<ul class="pagination justify-content-center">
								<li class="page-item" id="cluster_list_prev"><a class="page-link" href="#">Previous</a></li>
								<li class="page-item" id="cluster_list_next"><a class="page-link" href="#">Next</a></li>
							</ul>
						</div>
						<div class="col-auto mr-2">
							<ul class="pagination justify-content-start">
								<li class="page-item active" data-pager="cluster" data-pagesize="25"><a class="page-link" href="#">25</a></li>
								<li class="page-item" data-pager="cluster" data-pagesize="50"><a class="page-link" href="#">50</a></li>
								<li class="page-item" data-pager="cluster" data-pagesize="100"><a class="page-link" href="#">100</a></li>
							</ul>
						</div>
					</div>
				</div>

				<div role="tabpanel" class="tab-pane" id="schemas">
					<div class="row m-3">
						<div class="col">
							<button class="btn btn-success float-left" data-toggle="modal" data-target="#add_schema">&#10133; New</button>
						</div>
						<div class="col-auto">
							<form class="form-inline my-2 my-lg-0">
								<input id="schema_filter" type="search" class="form-control mr-sm-2" placeholder="Search">
							</form>
						</div>
					</div>
					<table class="table m-2" id="schema_list">
						<thead>
							<tr>
								<th scope="col"></th>
								<th scope="col">Name</th>
								<th scope="col">Type</th>
								<th scope="col">Schema</th>
								<th scope="col"></th>
							</tr>
						</thead>
						<tbody>
						</tbody>
					</table>
					<div class="row">
						<div class="col">
							<ul class="pagination justify-content-center">
								<li class="page-item" id="schema_list_prev"><a class="page-link" href="#">Previous</a></li>
								<li class="page-item" id="schema_list_next"><a class="page-link" href="#">Next</a></li>
							</ul>
						</div>
						<div class="col-auto mr-2">
							<ul class="pagination justify-content-start">
								<li class="page-item active" data-pager="schema" data-pagesize="25"><a class="page-link" href="#">25</a></li>
								<li class="page-item" data-pager="schema" data-pagesize="50"><a class="page-link" href="#">50</a></li>
								<li class="page-item" data-pager="schema" data-pagesize="100"><a class="page-link" href="#">100</a></li>
							</ul>
						</div>
					</div>
				</div>

				<div role="tabpanel" class="tab-pane" id="options">
<div class="container m-3">
							<div class="alert alert-danger collapse" id="config_error" role="alert"></div>
							<div class="alert alert-success collapse" id="config_success" role="alert">Successfully saved</div>
					<textarea class="form-control mb-2" rows="30" cols="80" wrap="soft" id="config_editor"></textarea>
					<button type="button" class="btn btn-primary" id="config_save">Save</button>
					<button type="button" class="btn btn-secondary" id="config_reset">Reset</button>
</div>
				</div>

			</div>
			<nav class="navbar navbar-dark bg-dark">
				<a class="navbar-brand" href="#">StorageTapper</a>
			</nav>
		</div>

		<div class="modal fade" id="add_table" tabindex="-1" role="dialog" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered" role="document">
				<div class="modal-content">
					<div class="modal-header">
						<h4 class="modal-title">New table</h4>
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
					</div>
					<form class="needs-validation" id="table_add" novalidate>
						<div class="modal-body">
							<div class="alert alert-danger collapse" id="table_add_error" role="alert"></div>
							<div class="row">
								<div class="col">
									<div class="form-group">
										<label>Input</label>
										<select class="form-control" name="input" id="inputType">
											<option>MySQL</option>
											<option>Schemaless</option>
										</select>
									</div>
								</div>
								<div class="col">
									<div class="form-group">
										<label>Format</label>
										<select class="form-control" name="outputFormat">
											<option>JSON</option>
											<option>MsgPack</option>
											<option>Avro</option>
											<option>MySQL</option>
											<option>AnsiSQL</option>
										</select>
									</div>
								</div>
								<div class="col">
									<div class="form-group">
										<label>Output</label>
										<select class="form-control" name="output" id="outputType">
											<option>Kafka</option>
											<option>HDFS</option>
											<option>Terrablob</option>
											<option>S3</option>
											<option>Bootstrap</option>
											<option>File</option>
											<option>MySQL</option>
											<option>Postgres</option>
											<option>ClickHouse</option>
										</select>
									</div>
								</div>
							</div>
							<div class="row">
								<div class="col">
									<div class="form-group">
										<label class="col-form-label">Service:</label>
										<input type="text" class="form-control" name="service" required>
										<small class="form-text text-muted">Put service name here</small>
										<div class="invalid-feedback">
											Service name is required
										</div>
									</div>
								</div>
								<div class="col">
									<div class="form-group">
										<label class="col-form-label">Cluster:</label>
										<input type="text" class="form-control" name="cluster" required>
										<small class="form-text text-muted">Put "*" to add all the clusters of the instance</small>
										<div class="invalid-feedback">
											Cluster name is required
										</div>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label class="col-form-label">Database:</label>
								<input type="text" class="form-control" name="db" required>
								<small class="form-text text-muted">Put "*" to add all the DB of the cluster</small>
								<div class="invalid-feedback">
									Database name is required
								</div>
							</div>
							<div class="form-group">
								<label class="col-form-label">Table:</label>
								<input type="text" class="form-control" name="table" required>
								<div class="invalid-feedback">
									Table name is required
								</div>
							</div>
							<div class="form-group">
								<label for="add_table_version" class="col-form-label">Version:</label>
								<div class="input-group">
									<input type="number" class="form-control" name="version" id="add_table_version">
									<div class="input-group-append">
										<button id="vergen" type="button" class="btn btn-outline-primary">Auto</button>
									</div>
								</div>
							</div>
							<div class="form-group">
								<label class="col-form-label">Parameters:</label>
								<div class="input-group">
									<input type="text" class="form-control" name="params">
								</div>
							</div>
							<div class="row">
								<div class="col">
									<label for="publishSchema" class="col-form-label">Publish current schema to</label>
								</div>
								<div class="col">
									<div class="form-group">
										<select class="form-control" name="publishSchema" id="publishSchema">
											<option></option>
											<option>State</option>
										</select>
									</div>
								</div>
							</div>
							<div class="form-check">
								<input class="form-check-input" type="checkbox" name="createTopic" id="createTopic" value="true" checked>
								<label for="createTopic" class="form-check-label">Create Kafka topic</label>
							</div>
						</div>
						<div class="modal-footer">
							<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
							<button type="submit" class="btn btn-primary" id="table_add_submit">Add</button>
						</div>
					</form>
				</div>
			</div>
		</div>

		<div class="modal fade" id="add_cluster" tabindex="-1" role="dialog" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered" role="document">
				<div class="modal-content">
					<div class="modal-header">
						<h4 class="modal-title">New cluster</h4>
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
					</div>
					<form class="needs-validation" id="cluster_add" novalidate>
						<div class="modal-body">
							<div class="alert alert-danger collapse" id="cluster_add_error" role="alert"></div>
							<div class="form-group">
								<label class="col-form-label">Name:</label>
								<input type="text" class="form-control" name="name" required autofocus>
								<small class="form-text text-muted">Put cluster name here</small>
								<div class="invalid-feedback">
									Cluster name is required
								</div>
							</div>
							<div class="form-group">
								<label class="col-form-label">Host:</label>
								<input type="text" class="form-control" name="host" required>
								<small class="form-text text-muted">Put host name here</small>
								<div class="invalid-feedback">
									Host name is required
								</div>
							</div>
							<div class="form-group">
								<label class="col-form-label">Port:</label>
								<input type="number" class="form-control" name="port" min="1" max="65535" value="3306">
								<small class="form-text text-muted">Put port here</small>
							</div>
							<div class="form-group">
								<label class="col-form-label">User:</label>
								<input type="text" class="form-control" name="user">
							</div>
							<div class="form-group">
								<label class="col-form-label">Password:</label>
								<input type="password" class="form-control" name="pw">
							</div>
						</div>
						<div class="modal-footer">
							<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
							<button type="submit" class="btn btn-primary" id="cluster_add_submit">Add</button>
						</div>
					</form>
				</div>
			</div>
		</div>

		<div class="modal fade" id="add_schema" tabindex="-1" role="dialog" aria-hidden="true">
			<div class="modal-dialog modal-dialog-centered" role="document">
				<div class="modal-content">
					<div class="modal-header">
						<h4 class="modal-title">New schema</h4>
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
					</div>
						<form class="needs-validation" id="schema_add" novalidate>
					<div class="modal-body">
							<div class="alert alert-danger collapse" id="schema_add_error" role="alert"></div>
							<div class="form-group">
								<label class="col-form-label">Name:</label>
								<input type="text" class="form-control" name="name" required>
								<small class="form-text text-muted">Schema name format: hp-tap-{service}-{db}-{table}</small>
							</div>
							<div class="form-group">
								<label>For format</label>
								<select class="form-control" name="type">
									<option>JSON</option>
									<option>MsgPack</option>
									<option>Avro</option>
								</select>
							</div>
							<div class="form-group">
								<label class="col-form-label">Schema:</label>
								<textarea rows="10" class="form-control" name="body" required></textarea>
								<small class="form-text text-muted">Schema in JSON format</small>
							</div>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
						<button type="submit" class="btn btn-primary" id="schema_add_submit">Add</button>
					</div>
						</form>
				</div>
			</div>
		</div>

		<div class="modal" tabindex="-1" role="dialog" id="confirm_delete">
			<div class="modal-dialog" role="document">
				<div class="modal-content">
					<div class="modal-header">
						<h5 class="modal-title">Confirm deletion</h5>
						<button type="button" class="close" data-dismiss="modal" aria-label="Close">
							<span aria-hidden="true">&times;</span>
						</button>
					</div>
					<div class="modal-body">
						<div class="alert alert-danger collapse" id="delete_error" role="alert"></div>
						<p id="confirm_msg">"Are you sure?"</p>
					</div>
					<div class="modal-footer">
						<button type="button" class="btn btn-primary" id="delete_yes">Delete</button>
						<button type="button" class="btn btn-secondary" data-dismiss="modal">Cancel</button>
					</div>
				</div>
			</div>
		</div>

	<script src="https://code.jquery.com/jquery-3.3.1.min.js" integrity="sha256-FgpCb/KJQlLNfOu91ta32o/NMZxltwRo8QtmkMRdAu8=" crossorigin="anonymous"></script>
	<script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.12.9/umd/popper.min.js" integrity="sha384-ApNbgh9B+Y1QKtv3Rn7W3mgPxhU9K/ScQsAP7hUibX39j7fakFPskvXusvfa0b4Q" crossorigin="anonymous"></script>
	<script src="https://maxcdn.bootstrapcdn.com/bootstrap/4.0.0/js/bootstrap.min.js" integrity="sha384-JZR6Spejh4U02d8jOt6vLEHfe/JQGiRRSQQxSfFWpi1MquVdAyjUar5+76PVCmYl" crossorigin="anonymous"></script>

<script>

var recs_per_page = { "table": 25, "cluster" : 25, "schema" : 25 };
var cur_page = { "table": 0, "cluster" : 0, "schema" : 0 };

function clusters_row(type, obj) {
	$("#"+type+"_list").append('<tr><th scope="row"></th> \
			<td>' + obj.name + '</td> \
			<td>' + obj.host + '</td> \
			<td>' + obj.port + '</td> \
			<td>' + obj.user + '</td> \
			<td style="width: 80px"><button class="btn btn-outline-danger" data-type="cluster" data-obj='+JSON.stringify(obj)+'>&#10006</button></td></tr>');
}

function color(s, n) {
	return "<font color="+(n ? "red" : "green")+">"+s+"</font>"
}

function tables_row(type, obj) {
	var u = new Date(obj.updatedAt), c = new Date(obj.createdAt)
	var s = new Date(obj.snapshottedAt), n = obj.needSnapshot
	$("#"+type+"_list").append('<tr><th scope="row"></th> \
	<td data-toggle="tooltip"title="UTC: '+u.toUTCString()+'\nCreated: '+c.toUTCString()+'">' + u.toLocaleString() + '</td> \
			<td>' + obj.service + '</td> \
			<td>' + obj.cluster + '</td> \
			<td>' + obj.db + '</td> \
			<td>' + obj.table + '</td> \
			<td>' + obj.input + '</td> \
			<td>' + obj.outputFormat + '</td> \
			<td>' + obj.output + '</td> \
			<td>' + obj.version + '</td> \
			<td>' + color(s.toISOString() == "0001-01-01T00:00:00.000Z" ? "never" : s.toLocaleString(),n) + '</td> \
			<td style="width: 80px"><button class="btn btn-outline-danger" data-type="table" data-obj='+JSON.stringify(obj)+'>&#10006</button></td></tr>');
}

function schemas_row(type, obj) {
	$("#"+type+"_list").append('<tr><th scope="row"></th> \
			<td>' + obj.name + '</td> \
			<td>' + obj.type + '</td> \
			<td>' + obj.body + '</td> \
			<td style="width: 80px"><button class="btn btn-outline-danger" data-type="schema" data-obj='+JSON.stringify(obj)+'>&#10006</button></td></tr>');
}

function load_page(type) {
	var offset = cur_page[type] * recs_per_page[type];
	var req = $.ajax({
		method: "POST",
		url:"/"+type,
		data: {"cmd" : "list",
			"filter" : $("#"+type+"_filter").val(),
			"offset" : cur_page[type] * recs_per_page[type],
			"limit" : recs_per_page[type]+1}
	}).done(function(response){
		$('#'+type+'_list tr').not(':first').remove();
//		$('#'+type+'_list_prev').prop("disabled", true)
//		$('#'+type+'_list_next').prop("disabled", true);
		if (!response) {
			if (cur_page[type] > 0) {
				cur_page[type]--
				load_page(type)
			}
			return;
		}
		var r = response.split("\n");
		for (var i in r) {
			if(r[i] && i < recs_per_page[type]) {
				var obj = JSON.parse(r[i]);
				if (type == "cluster")
					clusters_row(type, obj);
				else if (type == "table")
					tables_row(type, obj);
				else if (type == "schema")
					schemas_row(type, obj);
			}
		}
		$('#'+type+'_list_prev').prop("disabled", !cur_page[type])
		$('#'+type+'_list_next').prop("disabled", r.length <= recs_per_page[type] + 1);
	});
}

function setup_handlers(type) {
	$('#add_'+type).on('show.bs.modal', function (e) { $('#'+type+'_add_error').hide() });
	$('#'+type+'_list_prev').click(function (e) { if($(this).prop('disabled')) return false; cur_page[type]--; load_page(type); });
	$('#'+type+'_list_next').click(function (e) { if($(this).prop('disabled')) return false; cur_page[type]++; load_page(type); });
	$('#'+type+'_filter').keyup(function (e) { cur_page[type] = 0; load_page(type); });

	$('#'+type+'_add').submit(function(e) {
		e.preventDefault();
		e.stopPropagation();
		$('#'+type+'_add')[0].classList.add('was-validated');
		if ($('#'+type+'_add')[0].checkValidity() === false) {
			return
		}

		$('#'+type+'_add_submit').prop("disabled", true)

			var req = $.ajax({
				method: "POST",
				url:"/"+type+"?cmd=add",
				data: $('#'+type+'_add').serialize(),
				dataType: 'text'
			}).done(function(response) {
				load_page(type);
				$('#add_'+type).modal('toggle');
			}).fail(function(jqXHR, st) {
				$('#'+type+'_add_error').html(jqXHR.responseText).show()
			}).always(function() {
				$('#'+type+'_add_submit').prop("disabled", false)
			});
	});

  $('#'+type+'_list').on('click', 'button', function(){
	  var obj = JSON.parse($(this).attr("data-obj"));
	  var type = $(this).attr("data-type");
	  $('#delete_yes').attr("data-type", type);
	  $('#delete_yes').attr("data-body", $.param(obj));
	  if (type == "table") {
		  $('#confirm_msg').html(
				  '<div class="row m-3">The table:</div>' +
				  '<div class="row m-3"><table>' +
				  "<tr><td>Service:</td><td>" + obj.service + "</td></tr>" + 
				  "<tr><td>Cluster:</td><td>" + obj.cluster + "</td></tr>" + 
				  "<tr><td>Db:</td><td>" + obj.db + "</td></tr>" + 
				  "<tr><td>Table:</td><td>" + obj.table + "</td></tr>" + 
				  "<tr><td>Input:</td><td>" + obj.input + "</td></tr>" + 
				  "<tr><td>Output:</td><td>" + obj.output + "</td></tr>" + 
				  "<tr><td>Vesion:</td><td>" + obj.version + "</td></tr>" + 
				  '</table></div><div class="row m-3"><p>will be deregistered</p>');
	  } else if (type == "cluster") {
		  $('#confirm_msg').html('Cluster \'' + obj.name + '\' will be deleted from the registry');
	  } else if (type == "schema") {
		  $('#confirm_msg').html('Schema \'' + obj.name + '\' will be deleted from the registry');
	  }
	  $('#confirm_delete').modal('toggle');
  });
}

function config_load() {
	$('#config_reset').prop("disabled", true)
	$('#config_save').prop("disabled", true)

	$('#config_error').hide()
	$('#config_success').hide()

	var req = $.ajax({
		method: "GET",
		url:"/config?cmd=get",
		dataType: 'text'
	}).done(function(response) {
		$('#config_editor').text(response)
	}).fail(function(jqXHR, st) {
		$('#config_error').html(jqXHR.responseText).show()
	}).always(function() {
		$('#config_reset').prop("disabled", false)
		$('#config_save').prop("disabled", false)
	});
}

(function() {
  'use strict';
  $("#vergen").click(function(){ $("#add_table_version").val(Math.round((new Date()).getTime()/1000)) });

  $('#confirm_delete').on('show.bs.modal', function (e) { $('#delete_error').hide() });

  $('#delete_yes').click(function(e) {
	  e.preventDefault();
	  e.stopPropagation();

	  $('#delete_yes').prop("disabled", true)

	  var body = $(this).attr("data-body");
	  var type = $(this).attr("data-type");

	  var req = $.ajax({
		  method: "POST",
		  url:"/"+type+"?cmd=del",
		  data: body,
		  dataType: 'text'
	  }).done(function(response) {
		  load_page(type);
		  $('#confirm_delete').modal('toggle');
	  }).fail(function(jqXHR, st) {
		  $('#delete_error').html(jqXHR.responseText).show()
	  }).always(function() {
		  $('#delete_yes').prop("disabled", false)
	  });
  });

  $("li[data-pager]").click(function (e) {
	  var t = $(this).attr("data-pager");
	  $("li[data-pager="+t+"]").removeClass("active");
	  $(this).addClass("active");
	  recs_per_page[t] = Number($(this).attr("data-pagesize"));
	  cur_page[t] = 0;
	  load_page(t);
  });

  $("#inputType").change(function () {
	  var m = $("#inputType option:selected").text() == "MySQL"
	  var k = $("#outputType option:selected").text() == "Kafka"
	  $("#publishSchema").prop("disabled", !m);
	  $("#createTopic").prop("disabled", !m || !k);
  }).change();

  $("#outputType").change(function () {
	var m = $("#inputType option:selected").text() == "MySQL"
	var k = $("#outputType option:selected").text() == "Kafka"
	$("#createTopic").prop("disabled", !m || !k);
	var t = $("#outputType option:selected").text();
	var s = t == "MySQL" || t == "Postgres" || t == "ClickHouse"
	$("#outputFormat").prop("disabled", s);
	if (t == "MySQL")
		$("#outputFormat").val("MySQL").prop("selected", true);
	else if (s)
		$("#outputFormat").val("AnsiSQL").prop("selected", true);
  }).change();

  $("#config_reset").click(function (e) {
	  config_load()
  });

  $("#config_save").click(function (e) {
	  $('#config_save').prop("disabled", true)
	  $('#config_reset').prop("disabled", true)

	  $('#config_error').hide()
	  $('#config_success').hide()

	  var req = $.ajax({
		  method: "POST",
		  url:"/config?cmd=set",
		  data: $('#config_editor').val(),
		  contentType: 'application/x-yaml'
	  }).done(function(response) {
		  $('#config_success').show()
	  }).fail(function(jqXHR, st) {
		  $('#config_error').html(jqXHR.responseText).show()
	  }).always(function() {
		  $('#config_reset').prop("disabled", false)
		  $('#config_save').prop("disabled", false)
	  });
  });

  setup_handlers("cluster");
  setup_handlers("schema");
  setup_handlers("table");
  load_page("cluster");
  load_page("schema");
  load_page("table");
  config_load();
})();
</script>

	</body>
</html>`

func indexCmd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	_, err := w.Write([]byte(indexHTML))
	log.E(err)
}
