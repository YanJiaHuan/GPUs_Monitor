# GPU Monitor - GitHub Pages Demo

This is a demo version of the GPU Monitor application designed to run on GitHub Pages with mocked data.

## 🚀 How to Deploy to GitHub Pages

### Option 1: Using GitHub Settings (Recommended)

1. **Push the `docs` folder to your GitHub repository:**
   ```bash
   git add docs/
   git commit -m "Add GitHub Pages demo"
   git push origin main
   ```

2. **Enable GitHub Pages:**
   - Go to your repository on GitHub
   - Click on **Settings** tab
   - Scroll down to **Pages** section (in the left sidebar)
   - Under **Source**, select:
     - Branch: `main`
     - Folder: `/docs`
   - Click **Save**

3. **Access your site:**
   - GitHub will provide a URL like: `https://YOUR-USERNAME.github.io/GPU_Monitor/`
   - It may take a few minutes for the site to become available

### Option 2: Using a Separate Branch

1. **Create and switch to a `gh-pages` branch:**
   ```bash
   git checkout -b gh-pages
   ```

2. **Copy the index.html to root:**
   ```bash
   cp docs/index.html ./index.html
   git add index.html
   git commit -m "Add GitHub Pages demo"
   git push origin gh-pages
   ```

3. **Configure GitHub Pages:**
   - Go to repository Settings → Pages
   - Select branch: `gh-pages`
   - Folder: `/ (root)`
   - Click Save

## 🎨 Features

This demo version includes:
- **Mocked GPU data** simulating a real cluster with:
  - 2x RTX 3090 devices (each with 2 GPUs)
  - 1x RTX 4090 device (with 2 GPUs)
  - 1x A100 device (with 2 GPUs)
- **Real-time updates** - Data refreshes every 15 seconds with slight variations
- **Interactive refresh button** - Click to manually update the display
- **Responsive design** - Works on desktop, tablet, and mobile
- **Same UI/UX** as the production version

## 🔧 Customization

To modify the mock data, edit the `mockData` object in `index.html` around line 290. You can:
- Add/remove devices
- Change GPU specifications
- Adjust utilization percentages
- Simulate error states

## 📝 Differences from Production

- **No backend required** - All data is mocked in JavaScript
- **No SSH connections** - Simulates remote GPU monitoring
- **Random variations** - Data changes slightly on each refresh to appear live
- **Static hosting** - Can be hosted on any static file server

## 🌐 Alternative Hosting Options

Besides GitHub Pages, you can also host this on:
- **Netlify**: Drag and drop the `docs` folder
- **Vercel**: Connect your GitHub repo
- **Cloudflare Pages**: Push to GitHub and connect
- **Any static host**: Just upload the `index.html` file

## 🔙 Running Locally

To test locally before deploying:

```bash
cd docs
python -m http.server 8080
# or
python3 -m http.server 8080
```

Then open: http://localhost:8080
