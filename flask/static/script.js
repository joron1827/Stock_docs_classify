document.getElementById('execute-button').addEventListener('click', function() {
    var text = document.getElementById('input-text').value;

    // AJAX 요청을 통해 텍스트를 서버로 전송
    var xhr = new XMLHttpRequest();
    xhr.open('POST', '/process_text', true);
    xhr.setRequestHeader('Content-Type', 'application/json');
    xhr.onreadystatechange = function() {
        if (xhr.readyState === XMLHttpRequest.DONE && xhr.status === 200) {
            var imageContainer = document.getElementById('image-container');
            var imageUrl = URL.createObjectURL(xhr.response); // 서버에서 받은 응답을 이미지 URL로 변환

            // 이미지를 HTML에 추가
            var image = document.createElement('img');
            image.src = imageUrl;
            imageContainer.innerHTML = '';
            imageContainer.appendChild(image);
        }
    };
    xhr.responseType = 'blob';
    
    var requestData = JSON.stringify({ 'text': text });
    xhr.send(requestData);
});
