+
+var Author = React.createClass({
+  getInitialState: function() {
+    return {name: ''};
+  },
+ componentDidMount: function(searchText) {
+    $.ajax({
+      url: this.props.url,
+      dataType: 'json',
+      type: 'GET',
+      success: function(data) {
+        this.setState({name: data['name']});
+      }.bind(this),
+      error: function(xhr, status, err) {
+        console.error(this.props.url, status, err.toString());
+      }.bind(this)
+    });
+  },
+  componentWillUnmount: function() {
+    this.serverRequest.abort();
+  },
+  render: function() {
+    // TODO
+    return (
+      <p className="help-block">Author: Hildebrando Escudero</p>
+    );
+  }
+});



var Thumbnail = React.createClass({
  render: function() {
    var divStyle = {
      height: '180px',
      width: '100%',
      display: 'block'
    };
    return (
        <div className="col-md-3 col-xs-6"><img src={this.props.link} alt=""  style={divStyle} /></div>
    );
  }
});


var ThumbnailRow = React.createClass({
  render: function() {
    var thumbnailNodes = [];
    var row = this.props.row;
    this.props.images.forEach(function(image, i) {
      var col = i * row;
      thumbnailNodes.push(<Thumbnail key={col} link={image.link} />);
    });
    return (
      <div className="row">
        {thumbnailNodes}
      </div>
    );
  }
});


var ThumbnailGrid = React.createClass({
  render: function() {
    var step = 4; // number of columns
    var images = this.props.images;
    var thumbnailRows = [];
    for (var i=0; i<images.length; i+=step) {
      var row = i / step + 1;
      thumbnailRows.push(
        <ThumbnailRow images={images.slice(i, i + step)} row={row} key={row} />
      );
    }
    return (
      <div className="simple-grid">
        {thumbnailRows}
      </div>
    );
  }
});


var SearchBar = React.createClass({
  handleSubmit: function(e) {
    e.preventDefault();
    var text = this.refs.searchTextInput.value.trim();
    if (!text) {
      return;
    }
    this.props.onUserInput(text);
  },
  
  render: function() {
    return (
      <div>
        <p className="lead">Image search</p>
        <form className="form-inline" method="get" onSubmit={this.handleSubmit} >
          <input
            type="text"
            className="form-control"
            name="t"
            ref="searchTextInput"
          />
          <button type="submit" className="btn btn-default">Search</button>
          <p className="help-block">Author: Hildebrando Escudero (Hildebrando.Escudero)</p>
        </form>
      </div>
    );
  }
});


var ImageSearchContainer = React.createClass({
  getInitialState: function() {
    return {
      totalResults: 0,
      searchText: '',
      images: []
    };
  },
  loadResultsFromServer: function(searchText) {
    $.ajax({
      url: this.props.url,
      dataType: 'json',
      type: 'GET',
      data: {
        num:50,
        t:searchText
      },
      success: function(data) {
        this.setState({images: data['items']});
      }.bind(this),
      error: function(xhr, status, err) {
        console.error(this.props.url, status, err.toString());
      }.bind(this)
    });
  },
  handleUserInput: function(searchText) {
    this.setState({searchText: searchText});
    this.loadResultsFromServer(searchText);
  },

  render: function() {
    return (
      <div>
        <SearchBar
          searchText={this.state.searchText}
          onUserInput={this.handleUserInput}
        />
        <ThumbnailGrid
          images={this.state.images}
        />
      </div>
    );
  }
});


var URL = '/api/search'

ReactDOM.render(
  <ImageSearchContainer url={URL} />,
  document.getElementById('search')
);
