import './style.css';

const form = document.querySelector<HTMLFormElement>('#open-route');
const route = document.querySelector<HTMLTextAreaElement>('#route');
const status = document.querySelector<HTMLParagraphElement>('#status');

if (!form || !route || !status) throw new Error('Trusted shell markup is incomplete');

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  const routeBlobBase64 = route.value.trim();
  if (!routeBlobBase64) {
    status.textContent = 'Paste a private RouteBlob first.';
    return;
  }

  status.textContent = 'Importing private route…';
  try {
    const opened = await window.veilidShell.openRoute(routeBlobBase64);
    status.textContent = `Opened ${opened.origin}`;
  } catch (error) {
    status.textContent = error instanceof Error ? error.message : String(error);
  }
});
