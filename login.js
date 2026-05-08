
const graph = document.getElementById('graph');
for (let i = 0; i < 48; i++) {
  const b = document.createElement('div');
  b.className = 'bar';
  b.style.height = (20 + Math.random() * 70) + '%';
  graph.appendChild(b);
}

let attempts = 0;

function buildHiddenFlag() {
  const key = 23;
  const encoded = [
    84, 67, 81, 108, 115, 114, 113, 118, 98, 123,
    99, 72, 112, 101, 118, 113, 118, 121, 118, 72,
    116, 101, 114, 115, 114, 121, 99, 126, 118, 123, 100, 110
  ];
  return String.fromCharCode(...encoded.map(v => v ^ key));
}

function doLogin() {
  const user = document.getElementById('username').value.trim();
  const pass = document.getElementById('password').value.trim();
  const card = document.getElementById('loginCard');
  const err = document.getElementById('errorAlert');
  const msg = document.getElementById('errorMsg');

 
  if (user === 'admin' && pass === 'admin') {
    err.classList.remove('show');
    document.getElementById('flagText').textContent = buildHiddenFlag();
    document.getElementById('dashboard').classList.add('show');
    return;
  }

  
  attempts++;
  card.classList.remove('shake');
  void card.offsetWidth; 
  card.classList.add('shake');
  err.classList.add('show');

  if (attempts >= 3) {
    msg.textContent = 'Invalid credentials. (Hint: try well-known defaults for this service)';
  } else {
    msg.textContent = 'Invalid username or password.';
  }
}


document.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') doLogin();
});
