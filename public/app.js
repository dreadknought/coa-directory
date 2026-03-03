// File: public/app.js
(function () {
  const dataUrl = "/coa-data.json";
  const treeElement = document.getElementById("coa-tree");
  const statusElement = document.getElementById("status-message");
  const expandAllButton = document.getElementById("expand-all-button");

  async function loadCoaData() {
    try {
      setStatus("Loading COA data...");
      const response = await fetch(dataUrl, {
        headers: { Accept: "application/json" },
      });

      if (!response.ok) {
        throw new Error(`Unable to load COA data. HTTP ${response.status}`);
      }

      const data = await response.json();

      if (!Array.isArray(data)) {
        throw new Error("COA data file must contain a top-level array.");
      }

      renderTree(data);
      treeElement.hidden = false;

      if (data.length === 0) {
        setStatus("The COA directory is empty.");
      } else {
        setStatus(`Loaded ${data.length} categor${data.length === 1 ? "y" : "ies"}.`);
      }
    } catch (error) {
      console.error(error);
      treeElement.hidden = true;
      setStatus(
        error instanceof Error ? error.message : "Unable to load the COA directory.",
        true
      );
    }
  }

  function setStatus(message, isError) {
    statusElement.textContent = message;
    statusElement.setAttribute("role", isError ? "alert" : "status");
  }

  function renderTree(categories) {
    treeElement.innerHTML = "";

    if (!categories.length) {
      const emptyMessage = document.createElement("p");
      emptyMessage.className = "empty-message";
      emptyMessage.textContent = "No COAs are currently listed.";
      treeElement.appendChild(emptyMessage);
      return;
    }

    categories.forEach((categoryItem) => {
      const categoryDetails = createDetails(categoryItem.category || "Uncategorized", "level-1");

      const productContainer = document.createElement("div");
      productContainer.setAttribute(
        "aria-label",
        `Products in ${categoryItem.category || "this category"}`
      );

      (categoryItem.products || []).forEach((productItem) => {
        const productDetails = createDetails(productItem.product || "Unnamed Product", "level-2");

        const lotContainer = document.createElement("div");
        lotContainer.setAttribute(
          "aria-label",
          `Lots for ${productItem.product || "this product"}`
        );

        (productItem.lots || []).forEach((lotItem) => {
          const lotLabel = lotItem.lotNumber ? `Lot: ${lotItem.lotNumber}` : "Lot: Unspecified";
          const lotDetails = createDetails(lotLabel, "level-3");

          const fileList = document.createElement("ul");
          fileList.className = "file-list";
          fileList.setAttribute(
            "aria-label",
            `Files for ${productItem.product || "this product"}, ${lotLabel}`
          );

          (lotItem.files || []).forEach((file) => {
            const listItem = document.createElement("li");
            const link = document.createElement("a");
            link.className = "file-link";
            link.href = file.url || "#";
            link.textContent = file.name || "Open COA PDF";
            link.target = "_blank";
            link.rel = "noopener noreferrer";
            link.setAttribute(
              "aria-label",
              `Open COA PDF for ${productItem.product || "this product"}, ${lotLabel}: ${file.name || "COA PDF"}`
            );

            listItem.appendChild(link);
            fileList.appendChild(listItem);
          });

          if (fileList.children.length > 0) {
            lotDetails.appendChild(fileList);
          }

          lotContainer.appendChild(lotDetails);
        });

        productDetails.appendChild(lotContainer);
        productContainer.appendChild(productDetails);
      });

      categoryDetails.appendChild(productContainer);
      treeElement.appendChild(categoryDetails);
    });
  }

  function createDetails(summaryText, levelClassName) {
    const details = document.createElement("details");
    details.className = levelClassName;

    const summary = document.createElement("summary");
    summary.textContent = summaryText;
    details.appendChild(summary);

    return details;
  }

  function toggleAllDetails() {
    const allDetails = Array.from(treeElement.querySelectorAll("details"));
    const shouldOpen = expandAllButton.dataset.mode !== "collapse";

    allDetails.forEach((detailsElement) => {
      detailsElement.open = shouldOpen;
    });

    expandAllButton.textContent = shouldOpen ? "Collapse all" : "Expand all";
    expandAllButton.dataset.mode = shouldOpen ? "collapse" : "expand";
  }

  expandAllButton.addEventListener("click", toggleAllDetails);
  loadCoaData();
})();
