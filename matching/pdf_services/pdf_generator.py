"""
PDF Generator for JV Matcher Reports
"""

from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, PageBreak
from datetime import datetime
import logging
from pathlib import Path
from typing import Dict, Optional
import os

from .data_validator import DataValidator, ValidationError
from .pdf_styles import create_pdf_styles
from .pdf_components import (
    create_cover_page,
    create_dashboard,
    create_match_pages,
    create_action_tracker,
    FooterCanvas
)

logger = logging.getLogger(__name__)


class PDFGenerationError(Exception):
    """Custom exception for PDF failures"""
    pass


class PDFGenerator:
    """
    Generate professional PDF reports for JV matches

    Usage:
        generator = PDFGenerator()
        pdf_path = generator.generate(member_data)
    """

    def __init__(self, output_dir: str = './outputs'):
        """
        Initialize PDF generator

        Args:
            output_dir: Directory to save PDFs
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.validator = DataValidator()
        logger.info(f"PDFGenerator initialized with output_dir: {output_dir}")

    def generate(
        self,
        member_data: Dict,
        member_id: Optional[str] = None
    ) -> str:
        """
        Generate PDF report

        Args:
            member_data: Member profile and matches
            member_id: Optional unique identifier

        Returns:
            Path to generated PDF

        Raises:
            PDFGenerationError: If generation fails
        """
        try:
            # Validate
            participant = member_data.get('participant', 'Unknown')
            logger.info(f"Validating data for {participant}")
            self.validator.validate_member_data(member_data)

            # Add date if missing
            if 'date' not in member_data:
                member_data['date'] = datetime.now().strftime("%B %d, %Y")

            # Create filename
            safe_participant = participant.replace(' ', '_').replace('/', '_')
            if member_id:
                filename = f"{member_id}_{safe_participant}_JV_Report.pdf"
            else:
                date_str = datetime.now().strftime("%Y%m%d")
                filename = f"{safe_participant}_{date_str}_JV_Report.pdf"

            output_path = self.output_dir / filename

            # Generate
            logger.info(f"Generating PDF: {output_path}")
            self._create_pdf(member_data, str(output_path))

            logger.info(f"PDF generated successfully: {output_path}")
            return str(output_path)

        except ValidationError as e:
            logger.error(f"Validation failed: {e}")
            raise PDFGenerationError(f"Invalid data: {str(e)}")
        except Exception as e:
            logger.exception("PDF generation failed")
            raise PDFGenerationError(f"Failed to generate PDF: {str(e)}")

    def _create_pdf(self, data: Dict, output_path: str):
        """Internal PDF creation"""

        # Create document
        doc = SimpleDocTemplate(
            output_path,
            pagesize=letter,
            rightMargin=0.75 * inch,
            leftMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch
        )

        # Get styles
        styles = create_pdf_styles()

        # Build document story
        story = []

        # Cover page with profile
        story.extend(create_cover_page(data, styles))

        # Executive dashboard
        story.extend(create_dashboard(data.get('matches', []), styles))

        # Detailed match pages
        story.extend(create_match_pages(data.get('matches', []), styles))

        # Action tracker
        story.extend(create_action_tracker(data.get('matches', []), styles))

        # Build the PDF with custom footer canvas
        doc.build(story, canvasmaker=FooterCanvas)

    def generate_to_bytes(self, member_data: Dict, member_id: Optional[str] = None) -> bytes:
        """
        Generate PDF and return as bytes (useful for web downloads)

        Args:
            member_data: Member profile and matches
            member_id: Optional unique identifier

        Returns:
            PDF content as bytes
        """
        import tempfile

        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
            tmp_path = tmp.name

        try:
            # Generate to temp file
            self._temp_output_dir = self.output_dir
            self.output_dir = Path(tempfile.gettempdir())

            pdf_path = self.generate(member_data, member_id)

            # Read bytes
            with open(pdf_path, 'rb') as f:
                pdf_bytes = f.read()

            # Cleanup
            os.remove(pdf_path)

            return pdf_bytes

        finally:
            self.output_dir = self._temp_output_dir
