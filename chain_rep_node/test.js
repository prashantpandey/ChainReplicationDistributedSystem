

var i = 5;
var int = setInterval(function() {
    console.log('Inside interval ' + i);
    i = i - 1;
    if(i == 0) {
        clearInterval(int);
    }
}, 2000);
