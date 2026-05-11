FROM python:3.10-slim

# Sistem güncellemeleri ve gereksinimler
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Bağımlılıkların önbelleğe alınarak kurulması (Layer caching optimizasyonu)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Proje dosyalarının aktarımı
COPY . .

# Modüllerin birbirini bulabilmesi için PYTHONPATH ayarı
ENV PYTHONPATH=/app

# Varsayılan komut (docker-compose içinden override edilecek)
CMD ["python", "--version"]